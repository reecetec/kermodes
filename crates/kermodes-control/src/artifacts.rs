//! The artifacts store: what builds produce, in an S3 bucket of its own (`plan::Artifacts`).
//! Build Jobs upload to presigned URLs, one object each, so they never hold its keys; the
//! controller reads built apps back for the UI and deletes builds nothing uses.

use std::collections::HashMap;
use std::io::Read;
use std::path::Component;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::http::Method;
use object_store::ObjectStoreExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::signer::Signer;
use tokio::sync::Mutex;

use crate::plan;

/// A built app's files by path within it (`index.html`, `assets/index-3f2a.js`).
pub type AppFiles = HashMap<String, Bytes>;

pub struct Artifacts {
    s3: AmazonS3,
    /// Built apps read back, by object: a build never changes once uploaded.
    apps: Mutex<HashMap<String, Arc<AppFiles>>>,
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
        Ok(Self {
            s3,
            apps: Mutex::new(HashMap::new()),
        })
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

    /// The files of the built app in `object`, read once and kept.
    pub async fn app(&self, object: &str) -> Result<Arc<AppFiles>, String> {
        if let Some(files) = self.apps.lock().await.get(object) {
            return Ok(files.clone());
        }
        let tar = self
            .s3
            .get(&Path::from(object))
            .await
            .map_err(|e| format!("cannot read {object}: {e}"))?
            .bytes()
            .await
            .map_err(|e| format!("cannot read {object}: {e}"))?;
        let files = Arc::new(files_of_tar(&tar)?);
        self.apps
            .lock()
            .await
            .insert(object.to_owned(), files.clone());
        Ok(files)
    }

    pub async fn delete(&self, object: &str) -> Result<(), String> {
        self.apps.lock().await.remove(object);
        match self.s3.delete(&Path::from(object)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(format!("cannot delete {object}: {e}")),
        }
    }
}

/// The regular files of a tar, by their path with `./` and the like taken off. A path that
/// climbs out (`..`) or is absolute is refused, as the tar of a build never holds one.
pub fn files_of_tar(tar: &[u8]) -> Result<AppFiles, String> {
    let unreadable = |e: std::io::Error| format!("an unreadable build: {e}");
    let mut files = HashMap::new();
    let mut archive = tar::Archive::new(tar);
    for entry in archive.entries().map_err(unreadable)? {
        let mut entry = entry.map_err(unreadable)?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry.path().map_err(unreadable)?.into_owned();
        let mut parts = Vec::new();
        for component in path.components() {
            match component {
                Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
                Component::CurDir => {}
                _ => return Err(format!("a build holds {}", path.display())),
            }
        }
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).map_err(unreadable)?;
        files.insert(parts.join("/"), Bytes::from(bytes));
    }
    Ok(files)
}

/// The file a request for `path` within an app gets, and its name: the file itself; for the
/// app's address or any other without a file extension (a route of the app's own), its page;
/// none for a missing file.
pub fn app_file<'a>(files: &'a AppFiles, path: &'a str) -> Option<(&'a str, &'a Bytes)> {
    if let Some(file) = files.get(path) {
        return Some((path, file));
    }
    let last = path.rsplit('/').next().unwrap_or_default();
    if last.contains('.') {
        return None;
    }
    files.get("index.html").map(|page| ("index.html", page))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tar(files: &[(&str, &str)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, text) in files {
            let mut header = tar::Header::new_gnu();
            header.set_size(text.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, path, text.as_bytes())
                .unwrap();
        }
        builder.into_inner().unwrap()
    }

    #[test]
    fn a_build_serves_its_files_and_its_page_for_its_own_routes() {
        let files = files_of_tar(&tar(&[
            ("./index.html", "<!doctype html>"),
            ("./assets/index-3f2a.js", "export {}"),
        ]))
        .unwrap();
        let name = |path| app_file(&files, path).map(|(name, _)| name);
        assert_eq!(name(""), Some("index.html"));
        assert_eq!(name("assets/index-3f2a.js"), Some("assets/index-3f2a.js"));
        // A route the app handles in the page, and a file that is not there.
        assert_eq!(name("orders/42"), Some("index.html"));
        assert_eq!(name("assets/missing.js"), None);
        assert_eq!(
            app_file(&files, "assets/index-3f2a.js").unwrap().1.as_ref(),
            b"export {}"
        );
    }
}
