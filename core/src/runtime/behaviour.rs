use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::time::Duration;

use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::{
    StreamProtocol, autonat, dcutr, gossipsub, identify, identity::Keypair, kad, mdns, ping, relay,
    request_response, swarm::NetworkBehaviour,
};
use serde::{Deserialize, Serialize};

use crate::config::NodeConfig;

/// CBOR 编码消息的 trait 约束
///
/// 用于 request-response 协议的请求和响应类型必须满足的条件：
/// - `Serialize` + `Deserialize`: CBOR 编解码
/// - `Send` + `Sync`: 跨线程传递
/// - `'static`: 可被 tokio::spawn 使用
pub trait CborMessage:
    std::fmt::Debug + Clone + Serialize + Send + Sync + for<'a> Deserialize<'a> + 'static
{
}
impl<T> CborMessage for T where
    T: std::fmt::Debug + Clone + Serialize + Send + Sync + for<'a> Deserialize<'a> + 'static
{
}

/// 核心网络行为
///
/// 组合了 P2P 网络所需的各种协议：
/// - `ping`: 心跳检测，保持连接活跃
/// - `identify`: 节点信息交换，获取对方设备信息
/// - `kad`: Kademlia DHT，分布式哈希表用于跨网络发现
/// - `mdns`: 局域网发现，无需中心服务器
/// - `relay_client`: 中继客户端，NAT 穿透备选方案
/// - `autonat`: AutoNAT v2 Client，检测外部地址是否可达
/// - `dcutr`: 打洞协调，实现 NAT 穿透
/// - `gossipsub`: GossipSub pub/sub 消息广播
#[derive(NetworkBehaviour)]
pub struct CoreBehaviour<Req, Resp>
where
    Req: CborMessage,
    Resp: CborMessage,
{
    pub ping: ping::Behaviour,
    pub identify: identify::Behaviour,
    pub kad: kad::Behaviour<kad::store::MemoryStore>,
    pub req_resp: request_response::cbor::Behaviour<Req, Resp>,
    pub mdns: mdns::tokio::Behaviour,
    pub relay_client: relay::client::Behaviour,
    pub autonat: autonat::v2::client::Behaviour,
    pub dcutr: dcutr::Behaviour,
    pub gossipsub: Toggle<gossipsub::Behaviour>,
}

impl<Req, Resp> CoreBehaviour<Req, Resp>
where
    Req: CborMessage,
    Resp: CborMessage,
{
    /// 创建核心网络行为
    ///
    /// # 参数
    /// - `keypair`: 节点密钥对，用于身份认证
    /// - `relay_client`: 中继客户端行为（由 SwarmBuilder 自动创建）
    /// - `config`: 节点配置
    ///
    /// # Panics
    /// 如果 mDNS 初始化失败（极少见，通常表示系统级问题）
    pub fn new(
        keypair: &Keypair,
        relay_client: relay::client::Behaviour,
        config: &NodeConfig,
    ) -> Self {
        let peer_id = keypair.public().to_peer_id();

        // ===== Ping =====
        let ping = ping::Behaviour::new(
            ping::Config::new()
                .with_interval(config.ping_interval)
                .with_timeout(config.ping_timeout),
        );

        // ===== Identify =====
        let identify = identify::Behaviour::new(
            identify::Config::new(config.protocol_version.clone(), keypair.public())
                .with_agent_version(config.agent_version.clone())
                .with_push_listen_addr_updates(true)
                .with_cache_size(100),
        );

        // ===== Kademlia DHT =====
        let mut kad_config = kad::Config::default();
        kad_config
            .set_query_timeout(config.kad_query_timeout)
            .set_record_ttl(Some(Duration::from_secs(3600)))
            .set_replication_factor(NonZeroUsize::new(3).unwrap())
            .set_publication_interval(Some(Duration::from_secs(3600)))
            .set_provider_record_ttl(Some(Duration::from_secs(3600)));

        let mut kad =
            kad::Behaviour::with_config(peer_id, kad::store::MemoryStore::new(peer_id), kad_config);

        if config.kad_server_mode {
            kad.set_mode(Some(kad::Mode::Server));
        }

        // ===== mDNS =====
        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
            .expect("mDNS initialization failed");

        // ===== AutoNAT v2 Client =====
        let autonat = autonat::v2::client::Behaviour::default();

        // ===== DCUtR =====
        let dcutr = dcutr::Behaviour::new(peer_id);

        // ===== Request-Response =====
        let req_resp = request_response::cbor::Behaviour::new(
            [(
                StreamProtocol::try_from_owned(config.req_resp_protocol.clone())
                    .expect("invalid req_resp_protocol"),
                request_response::ProtocolSupport::Full,
            )],
            request_response::Config::default().with_request_timeout(config.req_resp_timeout),
        );

        // ===== GossipSub =====
        let gossipsub = if config.enable_gossipsub {
            // 基于内容的消息去重：相同内容只处理一次
            let message_id_fn = |message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
            };

            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(config.gossipsub_heartbeat_interval)
                .validation_mode(gossipsub::ValidationMode::Strict)
                .message_id_fn(message_id_fn)
                .build()
                .expect("valid gossipsub config");

            let behaviour = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                gossipsub_config,
            )
            .expect("valid gossipsub behaviour");

            Toggle::from(Some(behaviour))
        } else {
            Toggle::from(None)
        };

        Self {
            ping,
            identify,
            kad,
            mdns,
            relay_client,
            autonat,
            dcutr,
            req_resp,
            gossipsub,
        }
    }
}
