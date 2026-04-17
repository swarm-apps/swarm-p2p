use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};

/// NAT 状态
///
/// 仅区分 Public 和 Unknown：AutoNAT v2 按地址逐一探测，
/// 单次失败无法断定节点在 NAT 后面，因此不设 Private 状态。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NatStatus {
    /// 公网可达（至少一个地址通过 AutoNAT 验证）
    Public,
    /// 未知（尚未探测或探测未成功）
    #[default]
    Unknown,
}

/// 对外暴露的节点事件
///
/// 泛型参数 `Req` 是 request-response 协议的请求类型，
/// 用于 `InboundRequest` 变体携带请求内容。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum NodeEvent<Req = ()> {
    /// 开始监听某个地址
    Listening { addr: Multiaddr },

    /// 发现 peers（mDNS）
    PeersDiscovered { peers: Vec<(PeerId, Multiaddr)> },

    /// peer 已连接
    #[serde(rename_all = "camelCase")]
    PeerConnected { peer_id: PeerId },

    /// peer 已断开
    #[serde(rename_all = "camelCase")]
    PeerDisconnected { peer_id: PeerId },

    /// 收到 identify 信息
    #[serde(rename_all = "camelCase")]
    IdentifyReceived {
        peer_id: PeerId,
        agent_version: String,
        protocol_version: String,
    },

    /// Ping 成功，返回延迟
    #[serde(rename_all = "camelCase")]
    PingSuccess {
        peer_id: PeerId,
        /// 往返延迟（毫秒）
        rtt_ms: u64,
    },

    /// NAT 状态变化
    #[serde(rename_all = "camelCase")]
    NatStatusChanged {
        /// 新的 NAT 状态
        status: NatStatus,
        /// 如果是公网，返回外部地址
        public_addr: Option<Multiaddr>,
    },

    /// DCUtR 打洞成功，连接已升级为直连
    #[serde(rename_all = "camelCase")]
    HolePunchSucceeded { peer_id: PeerId },

    /// DCUtR 打洞失败
    #[serde(rename_all = "camelCase")]
    HolePunchFailed {
        peer_id: PeerId,
        /// 失败原因
        error: String,
    },

    /// Relay 预约已被接受，本节点可通过中继被连接
    #[serde(rename_all = "camelCase")]
    RelayReservationAccepted {
        relay_peer_id: PeerId,
        /// 是否为续约（而非首次预约）
        renewal: bool,
    },

    /// 收到对端的 request-response 请求
    #[serde(rename_all = "camelCase")]
    InboundRequest {
        peer_id: PeerId,
        /// 用于回复的唯一标识（传回 `NetClient::send_response` 时使用）
        pending_id: u64,
        /// 请求内容
        request: Req,
    },

    /// 收到 GossipSub 消息
    #[serde(rename_all = "camelCase")]
    GossipMessage {
        /// 消息原始发送者（签名验证后提取）
        source: Option<PeerId>,
        /// topic 名称
        topic: String,
        /// 消息内容
        #[serde(with = "serde_bytes_base64")]
        data: Vec<u8>,
    },

    /// 远端节点订阅了某个 GossipSub topic
    #[serde(rename_all = "camelCase")]
    GossipSubscribed { peer_id: PeerId, topic: String },

    /// 远端节点退订了某个 GossipSub topic
    #[serde(rename_all = "camelCase")]
    GossipUnsubscribed { peer_id: PeerId, topic: String },
}

/// 用于序列化 Vec<u8> 为 base64 字符串（serde JSON 兼容）
mod serde_bytes_base64 {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(data: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
        data.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        Vec::<u8>::deserialize(deserializer)
    }
}
