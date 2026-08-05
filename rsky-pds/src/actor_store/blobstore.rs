use crate::actor_store::aws::s3::S3BlobStore;
use crate::actor_store::disk_blobstore::DiskBlobStore;
use crate::config::BlobstoreConfig;
use aws_config::SdkConfig;
use std::path::Path;
use std::sync::Arc;

pub use rsky_blobstore::{BlobNotFoundError, BlobStore, BoxedBlobStream, MemoryBlobStore};

type DynBlobStore = dyn BlobStore<Stream = BoxedBlobStream>;

/// Builds the configured blobstore implementation for a given actor.
pub struct BlobstoreFactory {
    cfg: BlobstoreConfig,
    aws_cfg: SdkConfig,
}

impl BlobstoreFactory {
    pub fn new(cfg: BlobstoreConfig, aws_cfg: SdkConfig) -> Self {
        BlobstoreFactory { cfg, aws_cfg }
    }

    pub fn blobstore(&self, did: String) -> Arc<DynBlobStore> {
        match &self.cfg {
            BlobstoreConfig::Disk {
                location,
                tmp_location,
            } => Arc::new(DiskBlobStore::new(
                did,
                Path::new(location),
                tmp_location.as_deref().map(Path::new),
                None,
            )),
            BlobstoreConfig::S3 { bucket } => {
                Arc::new(S3BlobStore::new(did, &self.aws_cfg, bucket.clone()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lexicon_cid::Cid;
    use rsky_common::ipld::sha256_to_cid;
    use sha2::{Digest, Sha256};

    fn cid_for(bytes: &[u8]) -> Cid {
        sha256_to_cid(Sha256::digest(bytes).to_vec())
    }

    #[tokio::test]
    async fn factory_builds_disk_store_from_disk_config() {
        let dir = tempfile::tempdir().unwrap();
        let location = dir.path().join("blobs");
        let factory = BlobstoreFactory::new(
            BlobstoreConfig::Disk {
                location: location.to_string_lossy().to_string(),
                tmp_location: None,
            },
            SdkConfig::builder().build(),
        );
        let store = factory.blobstore("did:example:alice".to_owned());
        let bytes = b"factory blob".to_vec();
        let cid = cid_for(&bytes);
        store.put_permanent(cid, bytes.clone()).await.unwrap();
        // bytes landed on disk under {location}/{did}/{cid}
        let stored_path = location.join("did:example:alice").join(cid.to_string());
        assert_eq!(std::fs::read(stored_path).unwrap(), bytes);
        assert!(store.delete_all().is_some());
    }

    #[tokio::test]
    async fn factory_builds_disk_store_with_custom_tmp_location() {
        let dir = tempfile::tempdir().unwrap();
        let location = dir.path().join("blobs");
        let tmp_location = dir.path().join("tmp");
        let factory = BlobstoreFactory::new(
            BlobstoreConfig::Disk {
                location: location.to_string_lossy().to_string(),
                tmp_location: Some(tmp_location.to_string_lossy().to_string()),
            },
            SdkConfig::builder().build(),
        );
        let store = factory.blobstore("did:example:alice".to_owned());
        let key = store.put_temp(b"temp blob".to_vec()).await.unwrap();
        assert!(tmp_location.join("did:example:alice").join(&key).is_file());
    }

    #[tokio::test]
    async fn factory_builds_s3_store_from_s3_config() {
        let aws_cfg = SdkConfig::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .build();
        let factory = BlobstoreFactory::new(
            BlobstoreConfig::S3 {
                bucket: Some("my-bucket".to_owned()),
            },
            aws_cfg.clone(),
        );
        // constructs without touching the network; s3 stores cannot delete_all
        let store = factory.blobstore("did:example:alice".to_owned());
        assert!(store.delete_all().is_none());

        let legacy = BlobstoreFactory::new(BlobstoreConfig::S3 { bucket: None }, aws_cfg);
        let store = legacy.blobstore("did:example:alice".to_owned());
        assert!(store.delete_all().is_none());
    }
}