use std::path::Path;
use std::process;
use std::time::Duration;

use clap::{Parser, Subcommand};
use url::Url;

#[derive(Parser)]
#[command(name = "s3lock", version, about = "A locking command using S3")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Lock an S3 object
    Lock {
        /// S3 URL of the object to lock, e.g., s3://bucket/lock-obj-key
        s3_url: String,

        /// Fail if the lock cannot be acquired within seconds
        #[arg(short, long)]
        wait: Option<u64>,

        /// Lock file output path (default: <lock-obj-key>.lock)
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Unlock an S3 object
    Unlock {
        /// Lock file path
        lock_file: String,
    },
}

fn parse_s3_url(s: &str) -> Result<(String, String), String> {
    let url = Url::parse(s).map_err(|e| format!("invalid S3 URL: {e}"))?;

    if url.scheme() != "s3" {
        return Err(format!("invalid S3 URL: {s}"));
    }

    let bucket = url
        .host_str()
        .ok_or_else(|| format!("invalid S3 URL: {s}"))?
        .to_string();

    let key = url.path().trim_start_matches('/').to_string();
    if key.is_empty() {
        return Err(format!("invalid S3 URL: {s}"));
    }

    Ok((bucket, key))
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Err(err) = run(cli).await {
        eprintln!("s3lock: error: {err}");
        process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&config);

    match cli.command {
        Commands::Lock {
            s3_url,
            wait,
            output,
        } => {
            let (bucket, key) = parse_s3_url(&s3_url)?;

            let output_path = output.unwrap_or_else(|| {
                let base = Path::new(&key)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(&key);
                format!("{base}.lock")
            });

            let obj = rs3lock::Object::new(s3_client, &bucket, &key);

            let lock = if let Some(wait_secs) = wait {
                if wait_secs > 0 {
                    let timeout = Duration::from_secs(wait_secs);
                    match tokio::time::timeout(timeout, obj.lock_wait(Duration::from_secs(1)))
                        .await
                    {
                        Ok(result) => result?,
                        Err(_) => return Err(rs3lock::S3LockError::LockAlreadyHeld.into()),
                    }
                } else {
                    obj.lock().await?
                }
            } else {
                obj.lock().await?
            };

            println!("{s3_url} has been locked");

            let json = lock.marshal_json()?;
            std::fs::write(&output_path, &json)?;

            println!("create {output_path}");
        }
        Commands::Unlock { lock_file } => {
            let data = std::fs::read(&lock_file)?;
            let lock = rs3lock::Lock::from_json(s3_client, &data)?;

            lock.unlock().await?;

            println!("{lock} has been unlocked");

            std::fs::remove_file(&lock_file)?;

            println!("delete {lock_file}");
        }
    }

    Ok(())
}
