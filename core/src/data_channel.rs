//! 通用数据通道类型。
//!
//! 基于 `libp2p::stream` 封装的应用无关字节流抽象。`libs/core` 只暴露传输
//! 元信息（peer / protocol / channel id / 方向）和底层 `AsyncRead + AsyncWrite`
//! 流，绝不引入任何应用语义（file / chunk / session / checkpoint）。
//!
//! 帧编解码、消息边界由下游应用（如 SwarmDrop 的 transfer-data 协议）实现。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use libp2p::{PeerId, Stream, StreamProtocol};
use parking_lot::Mutex;
use tokio::sync::mpsc;

/// 数据通道唯一 ID。
///
/// 进程内自增，仅用于日志、生命周期跟踪和 limit 记账，不跨网络传输。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataChannelId(pub u64);

static CHANNEL_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

impl DataChannelId {
    /// 分配下一个进程内唯一 ID。
    pub(crate) fn next() -> Self {
        Self(CHANNEL_ID_COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

impl std::fmt::Display for DataChannelId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dc#{}", self.0)
    }
}

/// 数据通道方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataChannelDirection {
    /// 本端主动打开。
    Outbound,
    /// 远端打开、本端接受。
    Inbound,
}

/// 一条数据通道：绑定 peer / protocol / id / 方向的双向字节流。
///
/// 底层是 `libp2p::Stream`（`AsyncRead + AsyncWrite`）。通过 [`DataChannel::stream_mut`]
/// 或 [`DataChannel::into_stream`] 访问字节流；元信息通过访问器读取。
///
/// 该 handle **只暴露传输元信息**，不携带 file / session / chunk / checkpoint 等
/// 应用语义字段。
pub struct DataChannel {
    id: DataChannelId,
    peer: PeerId,
    protocol: StreamProtocol,
    direction: DataChannelDirection,
    stream: Stream,
    /// 配额 guard，drop 时释放 per-peer / per-protocol 计数。
    _guard: Option<ChannelGuard>,
}

impl DataChannel {
    pub(crate) fn new(
        id: DataChannelId,
        peer: PeerId,
        protocol: StreamProtocol,
        direction: DataChannelDirection,
        stream: Stream,
        guard: Option<ChannelGuard>,
    ) -> Self {
        Self {
            id,
            peer,
            protocol,
            direction,
            stream,
            _guard: guard,
        }
    }

    /// 通道 ID。
    pub fn id(&self) -> DataChannelId {
        self.id
    }

    /// 对端 peer。
    pub fn peer(&self) -> PeerId {
        self.peer
    }

    /// 协议名。
    pub fn protocol(&self) -> &StreamProtocol {
        &self.protocol
    }

    /// 方向。
    pub fn direction(&self) -> DataChannelDirection {
        self.direction
    }

    /// 可变借用底层字节流（`AsyncRead + AsyncWrite`）。
    pub fn stream_mut(&mut self) -> &mut Stream {
        &mut self.stream
    }

    /// 取出底层字节流，消费 handle。
    pub fn into_stream(self) -> Stream {
        self.stream
    }
}

impl std::fmt::Debug for DataChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataChannel")
            .field("id", &self.id)
            .field("peer", &self.peer)
            .field("protocol", &self.protocol.as_ref())
            .field("direction", &self.direction)
            .finish_non_exhaustive()
    }
}

/// 入站数据通道（runtime event）。
///
/// 由 core 在接受远端打开的通道后，通过 in-process receiver 转交给运行时消费者。
/// **不可序列化**——stream handle 不能进入 UI / 序列化事件 payload。
#[derive(Debug)]
pub struct InboundDataChannel {
    /// 接受到的数据通道。
    pub channel: DataChannel,
}

/// 数据通道关闭 / 失败原因（typed）。
///
/// 供下游状态机映射为 interrupted / retryable / fatal / local-cancelled。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataChannelCloseReason {
    /// 正常到达 end-of-stream。
    Normal,
    /// 底层 peer 连接在通道完成前关闭。
    ConnectionClosed,
    /// 本地调用方取消。
    LocalCancelled,
    /// 协议协商失败（对端不支持该协议）。
    UnsupportedProtocol,
    /// 超出 per-peer / per-protocol 资源限制。
    ResourceLimitExceeded,
    /// 其它 IO 错误（debug 字符串，归一化后保留细节）。
    Io(String),
}

impl std::fmt::Display for DataChannelCloseReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normal => write!(f, "normal end-of-stream"),
            Self::ConnectionClosed => write!(f, "underlying connection closed"),
            Self::LocalCancelled => write!(f, "locally cancelled"),
            Self::UnsupportedProtocol => write!(f, "unsupported protocol"),
            Self::ResourceLimitExceeded => write!(f, "resource limit exceeded"),
            Self::Io(detail) => write!(f, "io error: {detail}"),
        }
    }
}

/// 单个 data-channel 协议的注册配置。
#[derive(Debug, Clone)]
pub struct DataChannelProtocolConfig {
    /// 协议名（如 `/swarmdrop/transfer-data/1`）。
    pub protocol: StreamProtocol,
}

impl DataChannelProtocolConfig {
    pub fn new(protocol: StreamProtocol) -> Self {
        Self { protocol }
    }
}

/// data-channel 活跃通道数量限制。
///
/// 由 runtime 层（持有 `Control` 的事件循环）用计数 / `Semaphore` 执行，
/// 超限时**显式拒绝并报 typed error**，而非依赖底层 muxer 的静默丢弃。
#[derive(Debug, Clone, Copy)]
pub struct DataChannelLimits {
    /// 每个 peer 最大入站通道数。
    pub max_inbound_per_peer: usize,
    /// 每个 peer 最大出站通道数。
    pub max_outbound_per_peer: usize,
    /// 每个协议最大活跃通道数。
    pub max_per_protocol: usize,
}

impl Default for DataChannelLimits {
    fn default() -> Self {
        Self {
            max_inbound_per_peer: 4,
            max_outbound_per_peer: 4,
            max_per_protocol: 64,
        }
    }
}

/// 活跃数据通道计数登记表，执行 per-peer / per-protocol 限制。
///
/// 由 runtime（出站 `NetClient`、入站 event loop）共享。超限时调用方应**显式拒绝
/// 并报 typed error / 丢弃流**，而非依赖底层 muxer 的静默丢弃。
#[derive(Clone)]
pub(crate) struct ChannelRegistry {
    limits: DataChannelLimits,
    inner: Arc<Mutex<RegistryInner>>,
}

#[derive(Default, Debug)]
struct RegistryInner {
    inbound_per_peer: HashMap<PeerId, usize>,
    outbound_per_peer: HashMap<PeerId, usize>,
    per_protocol: HashMap<StreamProtocol, usize>,
}

impl ChannelRegistry {
    pub(crate) fn new(limits: DataChannelLimits) -> Self {
        Self {
            limits,
            inner: Arc::new(Mutex::new(RegistryInner::default())),
        }
    }

    /// 尝试为一条新通道占用配额。成功返回 guard（drop 时自动释放计数），超限返回 `None`。
    pub(crate) fn try_acquire(
        &self,
        peer: PeerId,
        protocol: StreamProtocol,
        direction: DataChannelDirection,
    ) -> Option<ChannelGuard> {
        let mut inner = self.inner.lock();
        let per_peer_limit = match direction {
            DataChannelDirection::Inbound => self.limits.max_inbound_per_peer,
            DataChannelDirection::Outbound => self.limits.max_outbound_per_peer,
        };
        let per_peer_count = match direction {
            DataChannelDirection::Inbound => {
                inner.inbound_per_peer.get(&peer).copied().unwrap_or(0)
            }
            DataChannelDirection::Outbound => {
                inner.outbound_per_peer.get(&peer).copied().unwrap_or(0)
            }
        };
        let per_protocol_count = inner.per_protocol.get(&protocol).copied().unwrap_or(0);
        if per_peer_count >= per_peer_limit || per_protocol_count >= self.limits.max_per_protocol {
            return None;
        }
        match direction {
            DataChannelDirection::Inbound => *inner.inbound_per_peer.entry(peer).or_default() += 1,
            DataChannelDirection::Outbound => {
                *inner.outbound_per_peer.entry(peer).or_default() += 1
            }
        }
        *inner.per_protocol.entry(protocol.clone()).or_default() += 1;
        Some(ChannelGuard {
            registry: self.inner.clone(),
            peer,
            protocol,
            direction,
        })
    }
}

/// 通道配额 guard，drop 时释放对应计数。
pub struct ChannelGuard {
    registry: Arc<Mutex<RegistryInner>>,
    peer: PeerId,
    protocol: StreamProtocol,
    direction: DataChannelDirection,
}

impl std::fmt::Debug for ChannelGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelGuard")
            .field("peer", &self.peer)
            .field("protocol", &self.protocol.as_ref())
            .field("direction", &self.direction)
            .finish()
    }
}

impl Drop for ChannelGuard {
    fn drop(&mut self) {
        let mut inner = self.registry.lock();
        let per_peer = match self.direction {
            DataChannelDirection::Inbound => &mut inner.inbound_per_peer,
            DataChannelDirection::Outbound => &mut inner.outbound_per_peer,
        };
        if let Some(c) = per_peer.get_mut(&self.peer) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                per_peer.remove(&self.peer);
            }
        }
        if let Some(c) = inner.per_protocol.get_mut(&self.protocol) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                inner.per_protocol.remove(&self.protocol);
            }
        }
    }
}

/// 入站数据通道接收器（runtime event 消费端）。
///
/// 由 `start()` 返回，运行时通过 [`DataChannelReceiver::recv`] 消费 core 接受到的
/// 入站通道。stream handle 走 in-process channel，不进入序列化事件路径。
pub struct DataChannelReceiver {
    rx: mpsc::Receiver<InboundDataChannel>,
}

impl DataChannelReceiver {
    pub(crate) fn new(rx: mpsc::Receiver<InboundDataChannel>) -> Self {
        Self { rx }
    }

    /// 接收下一个入站数据通道。
    pub async fn recv(&mut self) -> Option<InboundDataChannel> {
        self.rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_peer() -> PeerId {
        libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id()
    }

    fn proto() -> StreamProtocol {
        StreamProtocol::new("/test/data/1")
    }

    #[test]
    fn channel_id_is_monotonic() {
        let a = DataChannelId::next();
        let b = DataChannelId::next();
        assert!(b.0 > a.0);
    }

    #[test]
    fn limits_default_values() {
        let l = DataChannelLimits::default();
        assert_eq!(l.max_inbound_per_peer, 4);
        assert_eq!(l.max_outbound_per_peer, 4);
        assert_eq!(l.max_per_protocol, 64);
    }

    #[test]
    fn registry_enforces_per_peer_inbound_limit() {
        let limits = DataChannelLimits {
            max_inbound_per_peer: 2,
            max_outbound_per_peer: 4,
            max_per_protocol: 64,
        };
        let reg = ChannelRegistry::new(limits);
        let peer = test_peer();
        let g1 = reg.try_acquire(peer, proto(), DataChannelDirection::Inbound);
        let g2 = reg.try_acquire(peer, proto(), DataChannelDirection::Inbound);
        let g3 = reg.try_acquire(peer, proto(), DataChannelDirection::Inbound);
        assert!(g1.is_some() && g2.is_some());
        assert!(g3.is_none(), "第三条入站通道应超出 per-peer 限制");
        // 释放一个配额后应能再次占用
        drop(g1);
        let g4 = reg.try_acquire(peer, proto(), DataChannelDirection::Inbound);
        assert!(g4.is_some(), "释放后应可再次占用配额");
    }

    #[test]
    fn registry_enforces_per_protocol_limit() {
        let limits = DataChannelLimits {
            max_inbound_per_peer: 10,
            max_outbound_per_peer: 10,
            max_per_protocol: 2,
        };
        let reg = ChannelRegistry::new(limits);
        let g1 = reg.try_acquire(test_peer(), proto(), DataChannelDirection::Inbound);
        let g2 = reg.try_acquire(test_peer(), proto(), DataChannelDirection::Inbound);
        let g3 = reg.try_acquire(test_peer(), proto(), DataChannelDirection::Inbound);
        assert!(g1.is_some() && g2.is_some());
        assert!(g3.is_none(), "第三条通道应超出 per-protocol 限制");
    }

    #[test]
    fn registry_enforces_per_peer_outbound_limit() {
        let limits = DataChannelLimits {
            max_inbound_per_peer: 4,
            max_outbound_per_peer: 1,
            max_per_protocol: 64,
        };
        let reg = ChannelRegistry::new(limits);
        let peer = test_peer();
        let g1 = reg.try_acquire(peer, proto(), DataChannelDirection::Outbound);
        let g2 = reg.try_acquire(peer, proto(), DataChannelDirection::Outbound);

        assert!(g1.is_some());
        assert!(g2.is_none(), "第二条出站通道应超出 per-peer 限制");
    }

    #[test]
    fn registry_per_protocol_limit_is_shared_across_directions() {
        let limits = DataChannelLimits {
            max_inbound_per_peer: 10,
            max_outbound_per_peer: 10,
            max_per_protocol: 1,
        };
        let reg = ChannelRegistry::new(limits);
        let peer = test_peer();
        let inbound = reg.try_acquire(peer, proto(), DataChannelDirection::Inbound);
        let outbound = reg.try_acquire(peer, proto(), DataChannelDirection::Outbound);

        assert!(inbound.is_some());
        assert!(
            outbound.is_none(),
            "per-protocol 限制应统计入站和出站总活跃数"
        );
    }

    #[test]
    fn registry_inbound_and_outbound_counts_are_independent() {
        let limits = DataChannelLimits {
            max_inbound_per_peer: 1,
            max_outbound_per_peer: 1,
            max_per_protocol: 64,
        };
        let reg = ChannelRegistry::new(limits);
        let peer = test_peer();
        let inb = reg.try_acquire(peer, proto(), DataChannelDirection::Inbound);
        let out = reg.try_acquire(peer, proto(), DataChannelDirection::Outbound);
        assert!(inb.is_some() && out.is_some(), "入站与出站计数应彼此独立");
    }

    #[test]
    fn registry_drop_guard_removes_empty_counters() {
        let reg = ChannelRegistry::new(DataChannelLimits::default());
        let peer = test_peer();
        let protocol = proto();
        let guard = reg
            .try_acquire(peer, protocol.clone(), DataChannelDirection::Inbound)
            .expect("first acquire should succeed");

        drop(guard);

        let inner = reg.inner.lock();
        assert!(!inner.inbound_per_peer.contains_key(&peer));
        assert!(!inner.per_protocol.contains_key(&protocol));
    }

    #[test]
    fn registry_zero_limit_rejects_immediately() {
        let limits = DataChannelLimits {
            max_inbound_per_peer: 0,
            max_outbound_per_peer: 0,
            max_per_protocol: 0,
        };
        let reg = ChannelRegistry::new(limits);

        assert!(
            reg.try_acquire(test_peer(), proto(), DataChannelDirection::Inbound)
                .is_none()
        );
        assert!(
            reg.try_acquire(test_peer(), proto(), DataChannelDirection::Outbound)
                .is_none()
        );
    }
}
