use std::time::Duration;

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use rs3lock::{Lock, Object, S3LockError};

async fn new_s3_client() -> Client {
    let creds = Credentials::new("test", "test", None, None, "test");
    let config = aws_sdk_s3::Config::builder()
        .region(Region::new("us-east-1"))
        .endpoint_url("http://localhost:9090")
        .credentials_provider(creds)
        .force_path_style(true)
        .behavior_version_latest()
        .build();

    Client::from_conf(config)
}

async fn delete_object(client: &Client, bucket: &str, key: &str) {
    let _ = client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await;
}

async fn get_object(client: &Client, bucket: &str, key: &str) -> Result<String, String> {
    let output = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| format!("{e:?}"))?;

    let body = output.body.collect().await.map_err(|e| e.to_string())?;
    Ok(String::from_utf8_lossy(&body.into_bytes()).to_string())
}

async fn put_object(client: &Client, bucket: &str, key: &str, body: &str) -> String {
    let output = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send()
        .await
        .expect("put_object failed");

    output.e_tag().unwrap_or_default().to_string()
}

#[tokio::test]
async fn test_lock() {
    let client = new_s3_client().await;
    delete_object(&client, "s3lock-test", "lock-obj").await;

    // Lock
    let obj = Object::new(client.clone(), "s3lock-test", "lock-obj");
    let lock = obj.lock().await.expect("lock failed");

    // Confirm that the lock object exists
    let body = get_object(&client, "s3lock-test", "lock-obj")
        .await
        .expect("get_object failed");
    assert!(
        regex::Regex::new(r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}")
            .unwrap()
            .is_match(&body),
        "expected UUID pattern, got: {body}"
    );

    // Unlock
    lock.unlock().await.expect("unlock failed");

    // Confirm that the lock object does not exist
    let result = get_object(&client, "s3lock-test", "lock-obj").await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err();
    assert!(
        err_msg.contains("NoSuchKey") || err_msg.contains("The specified key does not exist"),
        "unexpected error: {err_msg}"
    );

    // Already unlocked
    let err = lock.unlock().await.unwrap_err();
    assert!(
        matches!(err, S3LockError::AlreadyUnlocked),
        "expected AlreadyUnlocked, got: {err}"
    );
}

#[tokio::test]
async fn test_lock_error() {
    let client = new_s3_client().await;
    delete_object(&client, "s3lock-test", "lock-obj").await;

    let obj = Object::new(client.clone(), "s3lock-test", "lock-obj");

    // Lock
    let lock = obj.lock().await.expect("lock failed");

    // Other clients cannot lock it
    let err = obj.lock().await.unwrap_err();
    assert!(
        matches!(err, S3LockError::LockAlreadyHeld),
        "expected LockAlreadyHeld, got: {err}"
    );

    // Unlock
    lock.unlock().await.expect("unlock failed");

    // Other clients can lock it
    obj.lock().await.expect("lock should succeed after unlock");
}

#[tokio::test]
async fn test_lock_fatal() {
    let client = new_s3_client().await;

    let obj = Object::new(client, "xxx-s3lock-test", "lock-obj");

    // Fatal error: bucket does not exist
    let err = obj.lock().await.unwrap_err();
    assert!(
        !matches!(err, S3LockError::LockAlreadyHeld),
        "expected non-LockAlreadyHeld error, got: {err}"
    );
    assert!(
        err.to_string().contains("specified bucket does not exist")
            || err.to_string().contains("NoSuchBucket"),
        "expected bucket not found error, got: {err}"
    );
}

#[tokio::test]
async fn test_marshal_json() {
    let client = new_s3_client().await;
    delete_object(&client, "s3lock-test", "lock-obj").await;

    // Lock
    let obj = Object::new(client.clone(), "s3lock-test", "lock-obj");
    let lock = obj.lock().await.expect("lock failed");

    let json = lock.marshal_json().expect("marshal failed");
    let json_str = String::from_utf8(json.clone()).unwrap();

    assert!(json_str.contains("\"Bucket\":\"s3lock-test\""));
    assert!(json_str.contains("\"Key\":\"lock-obj\""));
    assert!(json_str.contains("\"Id\":"));
    assert!(json_str.contains("\"ETag\":"));

    let lock2 = Lock::from_json(client.clone(), &json).expect("from_json failed");
    let json2 = lock2.marshal_json().expect("marshal failed");
    assert_eq!(json, json2);

    // Other clients cannot lock it
    let err = obj.lock().await.unwrap_err();
    assert!(matches!(err, S3LockError::LockAlreadyHeld));

    // Unlock (via deserialized lock)
    lock2.unlock().await.expect("unlock failed");

    // Confirm that the lock object does not exist
    let result = get_object(&client, "s3lock-test", "lock-obj").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_md5_collision() {
    let client = new_s3_client().await;
    delete_object(&client, "s3lock-test", "lock-obj").await;

    let id1 = "TEXTCOLLBYfGiJUETHQ4hAcKSMd5zYpgqf1YRDhkmxHkhPWptrkoyz28wnI9V0aHeAuaKnak";
    let id2 = "TEXTCOLLBYfGiJUETHQ4hEcKSMd5zYpgqf1YRDhkmxHkhPWptrkoyz28wnI9V0aHeAuaKnak";
    assert_ne!(id1, id2);

    // Manually put the lock object
    let etag = put_object(&client, "s3lock-test", "lock-obj", id1).await;
    let escaped_etag = etag.replace('"', "\\\"");

    // Create locks with the same MD5 hash
    let json1 = format!(
        r#"{{"Bucket":"s3lock-test","Key":"lock-obj","Id":"{id1}","ETag":"{escaped_etag}"}}"#
    );
    let json2 = format!(
        r#"{{"Bucket":"s3lock-test","Key":"lock-obj","Id":"{id2}","ETag":"{escaped_etag}"}}"#
    );

    let lock1 = Lock::from_json(client.clone(), json1.as_bytes()).expect("from_json failed");
    let lock2 = Lock::from_json(client.clone(), json2.as_bytes()).expect("from_json failed");

    // Unlock with a different lock id
    let err = lock2.unlock().await.unwrap_err();
    assert!(
        matches!(err, S3LockError::LockMismatch),
        "expected LockMismatch, got: {err}"
    );

    // Confirm that the lock object exists
    let body = get_object(&client, "s3lock-test", "lock-obj")
        .await
        .expect("get_object failed");
    assert_eq!(body, id1);

    // Unlock with a same lock id
    lock1.unlock().await.expect("unlock failed");

    // Confirm that the lock object does not exist
    let result = get_object(&client, "s3lock-test", "lock-obj").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_lock_mismatch() {
    let client = new_s3_client().await;
    put_object(&client, "s3lock-test", "lock-obj", "my-id").await;

    let lock = Lock::from_json(
        client,
        br#"{"Bucket":"s3lock-test","Key":"lock-obj","Id":"my-id","ETag":"invalid"}"#,
    )
    .expect("from_json failed");

    // Unlock with a different ETag
    let err = lock.unlock().await.unwrap_err();
    assert!(
        matches!(err, S3LockError::LockMismatch),
        "expected LockMismatch, got: {err}"
    );
}

#[tokio::test]
async fn test_lock_wait_1st_ok() {
    let client = new_s3_client().await;
    delete_object(&client, "s3lock-test", "lock-obj").await;

    let obj = Object::new(client, "s3lock-test", "lock-obj");
    let result = tokio::time::timeout(Duration::from_secs(5), obj.lock_wait(Duration::from_millis(100)))
        .await
        .expect("timeout");
    let lock = result.expect("lock_wait failed");
    assert!(lock.to_string().contains("s3lock-test"));
}

#[tokio::test]
async fn test_lock_wait_2nd_ok() {
    let client = new_s3_client().await;
    delete_object(&client, "s3lock-test", "lock-obj").await;

    let obj = Object::new(client, "s3lock-test", "lock-obj");

    let lock = obj.lock().await.expect("lock failed");

    // Unlock after a delay
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        lock.unlock().await.expect("unlock failed");
    });

    let result = tokio::time::timeout(Duration::from_secs(5), obj.lock_wait(Duration::from_millis(100)))
        .await
        .expect("timeout");
    let lock2 = result.expect("lock_wait should succeed");
    assert!(lock2.to_string().contains("s3lock-test"));
}

#[tokio::test]
async fn test_lock_wait_error() {
    let client = new_s3_client().await;
    delete_object(&client, "s3lock-test", "lock-obj").await;

    let obj = Object::new(client, "s3lock-test", "lock-obj");

    // Not unlock
    let _lock = obj.lock().await.expect("lock failed");

    let result = tokio::time::timeout(Duration::from_secs(2), obj.lock_wait(Duration::from_millis(100))).await;
    assert!(result.is_err(), "expected timeout");
}

#[tokio::test]
async fn test_lock_wait_fatal() {
    let client = new_s3_client().await;

    let obj = Object::new(client, "xxx-s3lock-test", "lock-obj");

    // Fatal error: bucket does not exist
    let result = tokio::time::timeout(Duration::from_secs(5), obj.lock_wait(Duration::from_millis(100)))
        .await
        .expect("timeout");
    let err = result.unwrap_err();
    assert!(
        !matches!(err, S3LockError::LockAlreadyHeld),
        "expected non-LockAlreadyHeld error"
    );
    assert!(
        err.to_string().contains("specified bucket does not exist")
            || err.to_string().contains("NoSuchBucket"),
        "expected bucket not found error, got: {err}"
    );
}

#[tokio::test]
async fn test_already_unlocked() {
    let client = new_s3_client().await;
    delete_object(&client, "s3lock-test", "lock-obj").await;

    // Create lock from JSON
    let lock = Lock::from_json(
        client,
        br#"{"Bucket":"s3lock-test","Key":"lock-obj","Id":"my-id","ETag":"\"my-etag\""}"#,
    )
    .expect("from_json failed");

    // Unlock
    let err = lock.unlock().await.unwrap_err();
    assert!(
        matches!(err, S3LockError::AlreadyUnlocked),
        "expected AlreadyUnlocked, got: {err}"
    );
}
