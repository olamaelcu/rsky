use anyhow::{bail, Result};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, Stream, StreamExt};
use lexicon_cid::Cid;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Debug, thiserror::Error)]
#[error("Blob not found")]
pub struct BlobNotFoundError;

/// The boxed default stream returned by [`BlobStore::get_stream`].
///
/// Implementations of [`BlobStore`] can either set
/// `type Stream = BoxedBlobStream;` and return whatever boxed stream they
/// produce, or override `Stream` with a more specific type (e.g.
/// `ReaderStream<File>`) — provided the trait is still used through a
/// `dyn BlobStore<Stream = BoxedBlobStream>` reference at the call site.
pub type BoxedBlobStream = BoxStream<'static, Result<Bytes>>;

/// Object storage for blob bytes, keyed by actor.
///
/// Mirrors the `BlobStore` interface from the reference implementation.
pub trait BlobStore: Send + Sync + Debug {
    type Stream: Stream<Item = Result<Bytes>> + Send + Unpin;

    fn put_temp(&self, bytes: Vec<u8>) -> BoxFuture<'_, Result<String>>;
    fn make_permanent(&self, key: String, cid: Cid) -> BoxFuture<'_, Result<()>>;
    fn put_permanent(&self, cid: Cid, bytes: Vec<u8>) -> BoxFuture<'_, Result<()>>;
    fn quarantine(&self, cid: Cid) -> BoxFuture<'_, Result<()>>;
    fn unquarantine(&self, cid: Cid) -> BoxFuture<'_, Result<()>>;
    fn get_bytes(&self, cid: Cid) -> BoxFuture<'_, Result<Vec<u8>>>;
    fn get_stream(&self, cid: Cid) -> BoxFuture<'_, Result<Self::Stream>>;
    fn has_temp(&self, key: String) -> BoxFuture<'_, Result<bool>>;
    fn has_stored(&self, cid: Cid) -> BoxFuture<'_, Result<bool>>;
    fn delete(&self, cid: Cid) -> BoxFuture<'_, Result<()>>;
    fn delete_many(&self, cids: Vec<Cid>) -> BoxFuture<'_, Result<()>>;
    /// Stores that can wipe an actor's blobs wholesale return a future;
    /// others return None and callers fall back to per-cid deletion.
    fn delete_all(&self) -> Option<BoxFuture<'_, Result<()>>> {
        None
    }
}

/// In-memory blobstore used by deterministic tests.
#[derive(Debug, Default)]
pub struct MemoryBlobStore {
    state: Mutex<MemoryBlobStoreState>,
    next_key: AtomicU64,
}

#[derive(Debug, Default)]
struct MemoryBlobStoreState {
    temp: HashMap<String, Bytes>,
    stored: HashMap<String, Bytes>,
    quarantined: HashMap<String, Bytes>,
}

impl MemoryBlobStore {
    fn lock(&self) -> std::sync::MutexGuard<'_, MemoryBlobStoreState> {
        self.state.lock().expect("memory blobstore mutex poisoned")
    }

    pub fn stored_cids(&self) -> Vec<String> {
        let mut cids: Vec<String> = self.lock().stored.keys().cloned().collect();
        cids.sort();
        cids
    }

    pub fn has_temp(&self, key: &str) -> bool {
        self.lock().temp.contains_key(key)
    }

    pub fn has_quarantined(&self, cid: &Cid) -> bool {
        self.lock().quarantined.contains_key(&cid.to_string())
    }
}

impl BlobStore for MemoryBlobStore {
    type Stream = BoxedBlobStream;

    fn put_temp(&self, bytes: Vec<u8>) -> BoxFuture<'_, Result<String>> {
        Box::pin(async move {
            let key = format!("temp-{}", self.next_key.fetch_add(1, Ordering::SeqCst));
            self.lock().temp.insert(key.clone(), Bytes::from(bytes));
            Ok(key)
        })
    }

    fn make_permanent(&self, key: String, cid: Cid) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut state = self.lock();
            let Some(bytes) = state.temp.remove(&key) else {
                bail!("temp blob not found: {key}")
            };
            state.stored.entry(cid.to_string()).or_insert(bytes);
            Ok(())
        })
    }

    fn put_permanent(&self, cid: Cid, bytes: Vec<u8>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.lock().stored.insert(cid.to_string(), Bytes::from(bytes));
            Ok(())
        })
    }

    fn quarantine(&self, cid: Cid) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut state = self.lock();
            let Some(bytes) = state.stored.remove(&cid.to_string()) else {
                bail!("stored blob not found: {cid}")
            };
            state.quarantined.insert(cid.to_string(), bytes);
            Ok(())
        })
    }

    fn unquarantine(&self, cid: Cid) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut state = self.lock();
            let Some(bytes) = state.quarantined.remove(&cid.to_string()) else {
                bail!("quarantined blob not found: {cid}")
            };
            state.stored.insert(cid.to_string(), bytes);
            Ok(())
        })
    }

    fn get_bytes(&self, cid: Cid) -> BoxFuture<'_, Result<Vec<u8>>> {
        Box::pin(async move {
            match self.lock().stored.get(&cid.to_string()) {
                Some(bytes) => Ok(bytes.to_vec()),
                None => bail!("stored blob not found: {cid}"),
            }
        })
    }

    fn get_stream(&self, cid: Cid) -> BoxFuture<'_, Result<Self::Stream>> {
        Box::pin(async move {
            let bytes = BlobStore::get_bytes(self, cid).await?;
            let stream = futures::stream::once(async move { Ok(Bytes::from(bytes)) }).boxed();
            Ok(stream)
        })
    }

    fn has_temp(&self, key: String) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move { Ok(self.lock().temp.contains_key(&key)) })
    }

    fn has_stored(&self, cid: Cid) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move { Ok(self.lock().stored.contains_key(&cid.to_string())) })
    }

    fn delete(&self, cid: Cid) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.lock().stored.remove(&cid.to_string());
            Ok(())
        })
    }

    fn delete_many(&self, cids: Vec<Cid>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut state = self.lock();
            for cid in cids {
                state.stored.remove(&cid.to_string());
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use sha2::{Digest, Sha256};

    const SHA2_256: u64 = 0x12;
    const RAWCODEC: u64 = 0x55;

    fn cid_for(bytes: &[u8]) -> Cid {
        let hash = Sha256::digest(bytes).to_vec();
        Cid::new_v1(RAWCODEC, lexicon_cid::multihash::Multihash::<64>::wrap(SHA2_256, &hash).unwrap())
    }

    #[tokio::test]
    async fn temp_to_permanent_lifecycle() {
        let store = MemoryBlobStore::default();
        let bytes = b"hello blob".to_vec();
        let cid = cid_for(&bytes);
        let key = store.put_temp(bytes.clone()).await.unwrap();
        assert!(store.has_temp(&key));
        assert!(BlobStore::has_temp(&store, key.clone()).await.unwrap());
        assert!(!store.has_stored(cid).await.unwrap());
        assert!(store.delete_all().is_none());

        store.make_permanent(key.clone(), cid).await.unwrap();
        assert!(!store.has_temp(&key));
        assert!(store.has_stored(cid).await.unwrap());
        assert_eq!(BlobStore::get_bytes(&store, cid).await.unwrap(), bytes);
        let streamed: Vec<Bytes> = BlobStore::get_stream(&store, cid)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(streamed.concat(), bytes);
        assert!(store.make_permanent(key, cid).await.is_err());
    }

    #[tokio::test]
    async fn get_stream_yields_same_bytes_as_get_bytes() {
        let store = MemoryBlobStore::default();
        let bytes = b"stream parity".to_vec();
        let cid = cid_for(&bytes);
        store.put_permanent(cid, bytes.clone()).await.unwrap();

        let direct = BlobStore::get_bytes(&store, cid).await.unwrap();
        let streamed: Vec<Bytes> = BlobStore::get_stream(&store, cid)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(direct, streamed.concat());
    }

    #[tokio::test]
    async fn quarantine_round_trip() {
        let store = MemoryBlobStore::default();
        let bytes = b"quarantine me".to_vec();
        let cid = cid_for(&bytes);
        assert!(store.quarantine(cid).await.is_err());
        store.put_permanent(cid, bytes).await.unwrap();
        store.quarantine(cid).await.unwrap();
        assert!(store.has_quarantined(&cid));
        assert!(!store.has_stored(cid).await.unwrap());
        assert!(BlobStore::get_bytes(&store, cid).await.is_err());
        assert!(BlobStore::get_stream(&store, cid).await.is_err());
        store.unquarantine(cid).await.unwrap();
        assert!(store.has_stored(cid).await.unwrap());
        assert!(store.unquarantine(cid).await.is_err());
    }

    #[tokio::test]
    async fn deletes_single_and_many() {
        let store = MemoryBlobStore::default();
        let one = b"one".to_vec();
        let two = b"two".to_vec();
        let (cid_one, cid_two) = (cid_for(&one), cid_for(&two));
        store.put_permanent(cid_one, one).await.unwrap();
        store.put_permanent(cid_two, two).await.unwrap();
        assert_eq!(store.stored_cids().len(), 2);
        store.delete(cid_one).await.unwrap();
        assert_eq!(store.stored_cids(), [cid_two.to_string()]);
        store.delete_many(vec![cid_one, cid_two]).await.unwrap();
        assert!(store.stored_cids().is_empty());
    }
}