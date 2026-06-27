use std::{
    collections::HashMap,
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tokio::time;

const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(10);

struct PendingEntry<V> {
    value: V,
    created_at: Instant,
}

/// 带 TTL 自动清理的并发 Map
///
/// 适用于跨 task 按 key 存取、一次性消费的场景（如 ResponseChannel 暂存）。
/// 内部启动一个 tokio 定时任务，周期性清理过期条目。
///
/// 使用 `Mutex<HashMap>` 而非 DashMap，因为 value 类型（如 `ResponseChannel`）
/// 可能不满足 `Sync` 约束。对于低竞争场景完全够用。
pub struct PendingMap<K, V> {
    inner: Arc<Mutex<HashMap<K, PendingEntry<V>>>>,
    ttl: Duration,
}

impl<K, V> Clone for PendingMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            ttl: self.ttl,
        }
    }
}

impl<K, V> PendingMap<K, V>
where
    K: Eq + Hash + Send + 'static,
    V: Send + 'static,
{
    pub fn new(ttl: Duration) -> Self {
        Self::with_cleanup_interval(ttl, DEFAULT_CLEANUP_INTERVAL)
    }

    fn with_cleanup_interval(ttl: Duration, cleanup_interval: Duration) -> Self {
        let map = Arc::new(Mutex::new(HashMap::new()));
        let map_clone = Arc::clone(&map);

        tokio::spawn(async move {
            let mut interval = time::interval(cleanup_interval);

            loop {
                interval.tick().await;
                purge_expired_entries(&mut map_clone.lock(), ttl);
            }
        });

        Self { inner: map, ttl }
    }

    pub fn insert(&self, key: K, value: V) {
        self.inner.lock().insert(
            key,
            PendingEntry {
                value,
                created_at: Instant::now(),
            },
        );
    }

    pub fn take(&self, key: &K) -> Option<V> {
        self.inner.lock().remove(key).map(|v| v.value)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// 立即清理过期条目，返回被移除的数量。
    pub fn purge_expired(&self) -> usize {
        purge_expired_entries(&mut self.inner.lock(), self.ttl)
    }
}

fn purge_expired_entries<K, V>(entries: &mut HashMap<K, PendingEntry<V>>, ttl: Duration) -> usize {
    let before = entries.len();
    let now = Instant::now();
    entries.retain(|_, v| now.duration_since(v.created_at) < ttl);
    before - entries.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn insert_and_take() {
        let map = PendingMap::new(Duration::from_secs(60));
        map.insert(1u64, "hello");
        map.insert(2, "world");

        assert_eq!(map.len(), 2);
        assert_eq!(map.take(&1), Some("hello"));
        assert_eq!(map.len(), 1);
        assert_eq!(map.take(&1), None); // 已经取出，不能再取
    }

    #[tokio::test]
    async fn take_nonexistent_returns_none() {
        let map = PendingMap::<u64, String>::new(Duration::from_secs(60));
        assert_eq!(map.take(&999), None);
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let map = PendingMap::new(Duration::from_secs(60));
        let map2 = map.clone();

        map.insert(1u64, "value");
        assert_eq!(map2.take(&1), Some("value")); // clone 共享底层数据
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn ttl_expiry_cleans_up() {
        let map = PendingMap::new(Duration::from_secs(60));
        map.inner.lock().insert(
            1u64,
            PendingEntry {
                value: "ephemeral",
                created_at: Instant::now() - Duration::from_secs(61),
            },
        );
        assert_eq!(map.len(), 1);

        assert_eq!(map.purge_expired(), 1);
        assert!(map.is_empty(), "expired entry should be cleaned up");
    }

    #[tokio::test]
    async fn non_expired_entries_survive_cleanup() {
        // TTL 足够长，条目不会被清理
        let map = PendingMap::new(Duration::from_secs(60));
        map.insert(1u64, "durable");

        tokio::task::yield_now().await;

        assert_eq!(map.len(), 1);
        assert_eq!(map.take(&1), Some("durable"));
    }

    #[tokio::test]
    async fn background_cleanup_uses_configured_interval() {
        let map = PendingMap::with_cleanup_interval(Duration::ZERO, Duration::from_millis(1));
        map.insert(1u64, "ephemeral");

        tokio::time::timeout(Duration::from_secs(1), async {
            while !map.is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("background cleanup should run within timeout");

        assert!(map.is_empty());
    }
}
