//! The artifacts store: what builds produce, in an S3 bucket of its own (`plan::Artifacts`).
//! Build Jobs upload to presigned URLs and pods download by them, one object each, so neither
//! holds its keys; the controller signs them and deletes builds nothing uses.

use std::time::Duration;

use axum::http::Method;
use object_store::ObjectStoreExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::signer::Signer;

use crate::plan;

pub struct Artifacts {
    s3: AmazonS3,
}

impl Artifacts {
    pub fn open(config: &plan::Artifacts) -> Result<Self, String> {
        let bucket = config
            .url
            .strip_prefix("s3://")
            .filter(|bucket| !bucket.is_empty() && !bucket.contains('/'))
            .ok_or_else(|| format!("the artifacts store {} is not s3://<bucket>", config.url))?;
        let key = |name: &str| {
            std::env::var(name).map_err(|_| format!("the artifacts store needs {name}"))
        };
        let s3 = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_endpoint(&config.endpoint)
            .with_allow_http(config.endpoint.starts_with("http://"))
            .with_region("us-east-1")
            .with_access_key_id(key("KERMODES_ARTIFACTS_ACCESS_KEY_ID")?)
            .with_secret_access_key(key("KERMODES_ARTIFACTS_SECRET_ACCESS_KEY")?)
            .build()
            .map_err(|e| format!("the artifacts store {}: {e}", config.url))?;
        Ok(Self { s3 })
    }

    /// A URL that uploads `object` with a PUT, valid for `ttl`, and nothing else.
    pub async fn upload_url(&self, object: &str, ttl: Duration) -> Result<String, String> {
        self.s3
            .signed_url(Method::PUT, &Path::from(object), ttl)
            .await
            .map(|url| url.to_string())
            .map_err(|e| format!("cannot sign an upload of {object}: {e}"))
    }

    /// A URL that downloads `object` with a GET, valid for `ttl`.
    pub async fn download_url(&self, object: &str, ttl: Duration) -> Result<String, String> {
        self.s3
            .signed_url(Method::GET, &Path::from(object), ttl)
            .await
            .map(|url| url.to_string())
            .map_err(|e| format!("cannot sign a download of {object}: {e}"))
    }

    /// Every object in the store, with its size in bytes.
    pub async fn list(&self) -> Result<Vec<(String, u64)>, String> {
        use futures_util::TryStreamExt;
        use object_store::ObjectStore;
        let objects: Vec<object_store::ObjectMeta> = self
            .s3
            .list(None)
            .try_collect()
            .await
            .map_err(|e| format!("cannot list the artifacts store: {e}"))?;
        Ok(objects
            .into_iter()
            .map(|meta| (meta.location.to_string(), meta.size))
            .collect())
    }

    /// Whether `object` was uploaded.
    pub async fn exists(&self, object: &str) -> Result<bool, String> {
        match self.s3.head(&Path::from(object)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(format!("cannot read {object}: {e}")),
        }
    }

    pub async fn delete(&self, object: &str) -> Result<(), String> {
        match self.s3.delete(&Path::from(object)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(format!("cannot delete {object}: {e}")),
        }
    }
}
