// Blobstore implementation backed by opendal so the same code can drive any
// opendal-supported backend (S3, GCS, Azure, local fs, sftp, ...) without
// pulling in each service's SDK directly.
use crate::blobstore::{BlobNotFoundError, BlobStore, BoxedBlobStream};
use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use lexicon_cid::Cid;
use opendal::{services, Operator};
use std::io::ErrorKind;

#[derive(Debug, Clone)]
pub struct OpendalBlobStore {
    op: Operator,
    pub did: String,
}

impl OpendalBlobStore {
    pub fn new(op: Operator, did: String) -> Self {
        OpendalBlobStore { op, did }
    }

    fn block_path(&self, cid: Cid) -> String {
        format!("blocks/{}/{}", self.did, cid)
    }

    fn tmp_path(&self, key: &str) -> String {
        format!("tmp/{}/{}", self.did, key)
    }

    fn quarantine_path(&self, cid: Cid) -> String {
        format!("quarantine/{}/{}", self.did, cid)
    }
}

/// Build an [`Operator`] for one of the supported opendal schemes. New schemes
/// can be added here without changing the blobstore trait.
pub fn build_operator(kind: &str, bucket: &str) -> Result<Operator> {
    let op = match kind {
        "fs" => Operator::new(services::Fs::default().root(bucket))?.finish(),
        "s3" => Operator::new(services::S3::default().bucket(bucket))?.finish(),
        other => bail!("unsupported opendal operator kind: {other}"),
    };
    Ok(op)
}

/// Convenience constructor: build an [`OpendalBlobStore`] in one call. Useful
/// for callers (e.g. rsky-pds's `BlobstoreFactory`) that don't already hold an
/// [`Operator`].
pub fn new_blobstore(kind: &str, bucket: &str, did: String) -> Result<OpendalBlobStore> {
    Ok(OpendalBlobStore::new(build_operator(kind, bucket)?, did))
}

fn translate_err(err: opendal::Error) -> anyhow::Error {
    if err.kind() == opendal::ErrorKind::NotFound {
        BlobNotFoundError.into()
    } else {
        anyhow!(err)
    }
}

fn gen_key() -> String {
    use rand::RngCore;
    format!("{:016x}", rand::thread_rng().next_u64())
}

impl BlobStore for OpendalBlobStore {
    type Stream = BoxedBlobStream;

    fn put_temp(&self, bytes: Vec<u8>) -> BoxFuture<'_, Result<String>> {
        let op = self.op.clone();
        let did = self.did.clone();
        Box::pin(async move {
            let key = gen_key();
            op.write(&format!("tmp/{did}/{key}"), bytes)
                .await
                .map_err(translate_err)?;
            Ok(key)
        })
    }

    fn make_permanent(&self, key: String, cid: Cid) -> BoxFuture<'_, Result<()>> {
        let op = self.op.clone();
        let did = self.did.clone();
        Box::pin(async move {
            let from = format!("tmp/{did}/{key}");
            let to = format!("blocks/{did}/{cid}");
            if op.exists(&to).await.map_err(translate_err)? {
                op.delete(&from).await.map_err(translate_err)?;
                return Ok(());
            }
            op.rename(&from, &to).await.map_err(translate_err)?;
            Ok(())
        })
    }

    fn put_permanent(&self, cid: Cid, bytes: Vec<u8>) -> BoxFuture<'_, Result<()>> {
        let op = self.op.clone();
        let did = self.did.clone();
        Box::pin(async move {
            op.write(&format!("blocks/{did}/{cid}"), bytes)
                .await
                .map_err(translate_err)?;
            Ok(())
        })
    }

    fn quarantine(&self, cid: Cid) -> BoxFuture<'_, Result<()>> {
        let op = self.op.clone();
        let did = self.did.clone();
        Box::pin(async move {
            op.rename(
                &format!("blocks/{did}/{cid}"),
                &format!("quarantine/{did}/{cid}"),
            )
            .await
            .map_err(translate_err)
        })
    }

    fn unquarantine(&self, cid: Cid) -> BoxFuture<'_, Result<()>> {
        let op = self.op.clone();
        let did = self.did.clone();
        Box::pin(async move {
            op.rename(
                &format!("quarantine/{did}/{cid}"),
                &format!("blocks/{did}/{cid}"),
            )
            .await
            .map_err(translate_err)
        })
    }

    fn get_bytes(&self, cid: Cid) -> BoxFuture<'_, Result<Vec<u8>>> {
        let op = self.op.clone();
        let did = self.did.clone();
        Box::pin(async move {
            let buf = op
                .read(&format!("blocks/{did}/{cid}"))
                .await
                .map_err(translate_err)?;
            Ok(buf.to_vec())
        })
    }

    fn get_stream(&self, cid: Cid) -> BoxFuture<'_, Result<BoxedBlobStream>> {
        let op = self.op.clone();
        let did = self.did.clone();
        Box::pin(async move {
            let reader = match op.reader(&format!("blocks/{did}/{cid}")).await {
                Ok(r) => r,
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                    return Err(BlobNotFoundError.into());
                }
                Err(e) => return Err(anyhow!(e)),
            };
            let mut async_read = reader.into_futures_async_read(..).await?;
            let stream = futures::stream::poll_fn(move |cx| {
                let mut buf = vec![0u8; 16 * 1024];
                let pinned = std::pin::Pin::new(&mut async_read);
                match futures::AsyncRead::poll_read(pinned, cx, &mut buf) {
                    std::task::Poll::Ready(Ok(0)) => std::task::Poll::Ready(None),
                    std::task::Poll::Ready(Ok(n)) => {
                        std::task::Poll::Ready(Some(Ok(Bytes::copy_from_slice(&buf[..n]))))
                    }
                    std::task::Poll::Ready(Err(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                        std::task::Poll::Ready(None)
                    }
                    std::task::Poll::Ready(Err(e)) => {
                        std::task::Poll::Ready(Some(Err(anyhow!(e))))
                    }
                    std::task::Poll::Pending => std::task::Poll::Pending,
                }
            });
            Ok(stream.boxed())
        })
    }

    fn has_temp(&self, key: String) -> BoxFuture<'_, Result<bool>> {
        let op = self.op.clone();
        let did = self.did.clone();
        Box::pin(async move {
            op.exists(&format!("tmp/{did}/{key}"))
                .await
                .map_err(translate_err)
        })
    }

    fn has_stored(&self, cid: Cid) -> BoxFuture<'_, Result<bool>> {
        let op = self.op.clone();
        let did = self.did.clone();
        Box::pin(async move {
            op.exists(&format!("blocks/{did}/{cid}"))
                .await
                .map_err(translate_err)
        })
    }

    fn delete(&self, cid: Cid) -> BoxFuture<'_, Result<()>> {
        let op = self.op.clone();
        let did = self.did.clone();
        Box::pin(async move {
            // opendal.delete is a no-op on missing paths; nothing to translate.
            op.delete(&format!("blocks/{did}/{cid}"))
                .await
                .map_err(translate_err)
        })
    }

    fn delete_many(&self, cids: Vec<Cid>) -> BoxFuture<'_, Result<()>> {
        let op = self.op.clone();
        let did = self.did.clone();
        Box::pin(async move {
            for cid in cids {
                let _ = op.delete(&format!("blocks/{did}/{cid}")).await;
            }
            Ok(())
        })
    }

    fn delete_all(&self) -> Option<BoxFuture<'_, Result<()>>> {
        let op = self.op.clone();
        let did = self.did.clone();
        Some(Box::pin(async move {
            for prefix in ["blocks", "tmp", "quarantine"] {
                let path = format!("{prefix}/{did}");
                op.remove_all(&path).await.map_err(translate_err)?;
            }
            Ok(())
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    const SHA2_256: u64 = 0x12;
    const RAWCODEC: u64 = 0x55;

    fn cid_for(bytes: &[u8]) -> Cid {
        let hash = Sha256::digest(bytes).to_vec();
        Cid::new_v1(
            RAWCODEC,
            lexicon_cid::multihash::Multihash::<64>::wrap(SHA2_256, &hash).unwrap(),
        )
    }

    fn temp_store() -> OpendalBlobStore {
        let dir = tempfile::tempdir().unwrap();
        let op = build_operator("fs", dir.path().to_str().unwrap()).unwrap();
        OpendalBlobStore::new(op, "did:example:alice".to_owned())
    }

    #[tokio::test]
    async fn temp_to_permanent_lifecycle() {
        let store = temp_store();
        let bytes = b"hello opendal blob".to_vec();
        let cid = cid_for(&bytes);
        let key = store.put_temp(bytes.clone()).await.unwrap();
        assert!(BlobStore::has_temp(&store, key.clone()).await.unwrap());
        assert!(!store.has_stored(cid).await.unwrap());
        store.make_permanent(key.clone(), cid).await.unwrap();
        assert!(!store.has_temp(key.clone()).await.unwrap());
        assert!(store.has_stored(cid).await.unwrap());
        assert_eq!(BlobStore::get_bytes(&store, cid).await.unwrap(), bytes);
        let streamed: Vec<Bytes> = BlobStore::get_stream(&store, cid)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(streamed.concat(), bytes);
    }

    #[tokio::test]
    async fn quarantine_round_trip() {
        let store = temp_store();
        let bytes = b"quarantine me".to_vec();
        let cid = cid_for(&bytes);
        store.put_permanent(cid, bytes).await.unwrap();
        store.quarantine(cid).await.unwrap();
        assert!(!store.has_stored(cid).await.unwrap());
        store.unquarantine(cid).await.unwrap();
        assert!(store.has_stored(cid).await.unwrap());
    }

    #[tokio::test]
    async fn missing_blob_returns_not_found() {
        let store = temp_store();
        let cid = cid_for(b"never uploaded");
        let err = BlobStore::get_bytes(&store, cid).await.unwrap_err();
        assert!(err.downcast_ref::<BlobNotFoundError>().is_some());
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let store = temp_store();
        let cid = cid_for(b"deletable");
        store.put_permanent(cid, b"deletable".to_vec()).await.unwrap();
        store.delete(cid).await.unwrap();
        store.delete(cid).await.unwrap();
    }
}