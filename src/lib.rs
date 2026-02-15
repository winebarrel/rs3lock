use std::fmt;
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::Response as HttpResponse;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum S3LockError {
    #[error("lock already held")]
    LockAlreadyHeld,
    #[error("already unlocked")]
    AlreadyUnlocked,
    #[error("lock mismatch")]
    LockMismatch,
    #[error("{0}")]
    Sdk(String),
}

impl S3LockError {
    fn from_sdk_error<E: fmt::Debug>(err: SdkError<E, HttpResponse>) -> Self {
        let status = match &err {
            SdkError::ServiceError(e) => Some(e.raw().status().as_u16()),
            SdkError::ResponseError(e) => Some(e.raw().status().as_u16()),
            _ => None,
        };

        match status {
            Some(412) => S3LockError::LockAlreadyHeld,
            _ => S3LockError::Sdk(format!("{err:?}")),
        }
    }

    fn from_sdk_error_validate<E: fmt::Debug>(err: SdkError<E, HttpResponse>) -> Self {
        let status = match &err {
            SdkError::ServiceError(e) => Some(e.raw().status().as_u16()),
            SdkError::ResponseError(e) => Some(e.raw().status().as_u16()),
            _ => None,
        };

        match status {
            Some(404) => S3LockError::AlreadyUnlocked,
            Some(412) => S3LockError::LockMismatch,
            _ => S3LockError::Sdk(format!("{err:?}")),
        }
    }
}

pub struct Object {
    s3: Client,
    bucket: String,
    key: String,
}

impl Object {
    pub fn new(s3_client: Client, bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Object {
            s3: s3_client,
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    pub async fn lock(&self) -> Result<Lock, S3LockError> {
        let id = Uuid::new_v4().to_string();

        let result = self
            .s3
            .put_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .body(id.as_bytes().to_vec().into())
            .if_none_match("*")
            .send()
            .await;

        match result {
            Ok(output) => {
                let etag = output.e_tag().unwrap_or_default().to_string();
                Ok(Lock {
                    s3: self.s3.clone(),
                    bucket: self.bucket.clone(),
                    key: self.key.clone(),
                    id,
                    etag,
                    unlocked: Mutex::new(false),
                })
            }
            Err(err) => Err(S3LockError::from_sdk_error(err)),
        }
    }

    pub async fn lock_wait(&self, interval: Duration) -> Result<Lock, S3LockError> {
        // first time
        match self.lock().await {
            Ok(lock) => return Ok(lock),
            Err(S3LockError::LockAlreadyHeld) => {}
            Err(err) => return Err(err),
        }

        // after the second time
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // consume the immediate first tick

        loop {
            ticker.tick().await;
            match self.lock().await {
                Ok(lock) => return Ok(lock),
                Err(S3LockError::LockAlreadyHeld) => {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }
}

pub struct Lock {
    s3: Client,
    bucket: String,
    key: String,
    id: String,
    etag: String,
    unlocked: Mutex<bool>,
}

impl fmt::Debug for Lock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lock")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("id", &self.id)
            .field("etag", &self.etag)
            .finish()
    }
}

impl fmt::Display for Lock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.key)
    }
}

#[derive(Serialize, Deserialize)]
struct LockJson {
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Id")]
    id: String,
    #[serde(rename = "ETag")]
    etag: String,
}

impl Lock {
    pub async fn unlock(&self) -> Result<(), S3LockError> {
        let mut unlocked = self.unlocked.lock().await;

        if *unlocked {
            return Err(S3LockError::AlreadyUnlocked);
        }

        // validate: get object and check content
        let result = self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .if_match(&self.etag)
            .send()
            .await;

        match result {
            Ok(output) => {
                let body = output
                    .body
                    .collect()
                    .await
                    .map_err(|e| S3LockError::Sdk(e.to_string()))?;
                let body_bytes = body.into_bytes();
                let content = String::from_utf8_lossy(&body_bytes);

                if *content != self.id {
                    return Err(S3LockError::LockMismatch);
                }
            }
            Err(err) => return Err(S3LockError::from_sdk_error_validate(err)),
        }

        // delete
        self.s3
            .delete_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .if_match(&self.etag)
            .send()
            .await
            .map_err(|e| S3LockError::Sdk(e.to_string()))?;

        *unlocked = true;

        Ok(())
    }

    pub fn marshal_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        let j = LockJson {
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            id: self.id.clone(),
            etag: self.etag.clone(),
        };
        serde_json::to_vec(&j)
    }

    pub fn from_json(s3_client: Client, data: &[u8]) -> Result<Self, serde_json::Error> {
        let j: LockJson = serde_json::from_slice(data)?;
        Ok(Lock {
            s3: s3_client,
            bucket: j.bucket,
            key: j.key,
            id: j.id,
            etag: j.etag,
            unlocked: Mutex::new(false),
        })
    }
}
