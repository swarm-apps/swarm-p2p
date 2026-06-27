use std::time::Duration;
use std::{fmt::Debug, num::NonZeroUsize};

use libp2p::{
    StreamProtocol, autonat, dcutr, identify, identity::Keypair, kad, mdns, ping, relay,
    request_response, swarm::NetworkBehaviour, swarm::behaviour::toggle::Toggle,
};
use serde::{Deserialize, Serialize};

use crate::config::{InfrastructureMode, LanHelperConfig, NodeConfig, RelayLimits};

/// CBOR 编码消息的 trait 约束
///
/// 用于 request-response 协议的请求和响应类型必须满足的条件：
/// - `Serialize` + `Deserialize`: CBOR 编解码
/// - `Send` + `Sync`: 跨线程传递
/// - `'static`: 可被 tokio::spawn 使用
pub trait CborMessage:
    Debug + Clone + Serialize + Send + Sync + for<'a> Deserialize<'a> + 'static
{
}
impl<T> CborMessage for T where
    T: Debug + Clone + Serialize + Send + Sync + for<'a> Deserialize<'a> + 'static
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
    pub mdns: Toggle<mdns::tokio::Behaviour>,
    pub relay_client: Toggle<relay::client::Behaviour>,
    pub relay_server: Toggle<relay::Behaviour>,
    pub autonat: Toggle<autonat::v2::client::Behaviour>,
    pub dcutr: Toggle<dcutr::Behaviour>,
    pub stream: libp2p_stream::Behaviour,
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
        // 定期发送心跳包检测连接是否存活
        // - interval: 心跳间隔，太短浪费带宽，太长检测不及时
        // - timeout: 超时时间，超过则认为连接断开
        let ping = ping::Behaviour::new(
            ping::Config::new()
                .with_interval(config.ping_interval)
                .with_timeout(config.ping_timeout),
        );

        // ===== Identify =====
        // 节点信息交换协议，连接建立后自动运行
        // - protocol_version: 协议版本，用于兼容性检查
        // - agent_version: 客户端版本，可包含设备信息
        // - push_listen_addr_updates: 地址变化时主动推送给已连接节点
        // - cache_size: 缓存最近 N 个节点的信息，避免重复请求
        let identify = identify::Behaviour::new(
            identify::Config::new(config.protocol_version.clone(), keypair.public())
                .with_agent_version(config.agent_version.clone())
                .with_push_listen_addr_updates(true)
                .with_cache_size(100),
        );

        // ===== Kademlia DHT =====
        // 分布式哈希表，用于：
        // 1. 跨网络节点发现（通过 PUT_PROVIDER/GET_PROVIDERS）
        // 2. 存储分享码等元数据（通过 PUT/GET）
        //
        // 配置说明：
        // - query_timeout: 查询超时，网络差时可适当增加
        // - record_ttl: 记录生存时间，过期自动清理
        // - replication_factor: 复制因子，存储到 N 个最近节点
        // - publication_interval: 定期重新发布，保持记录有效
        // - provider_record_ttl: Provider 记录的 TTL
        let mut kad_config = kad::Config::default();
        kad_config
            .set_query_timeout(config.kad_query_timeout)
            .set_record_ttl(Some(Duration::from_secs(3600))) // 1 小时
            .set_replication_factor(NonZeroUsize::new(3).unwrap())
            .set_publication_interval(Some(Duration::from_secs(3600)))
            .set_provider_record_ttl(Some(Duration::from_secs(3600)));

        let mut kad =
            kad::Behaviour::with_config(peer_id, kad::store::MemoryStore::new(peer_id), kad_config);

        // 默认 Kad 模式由 AutoNAT 自动判定（确认公网可达后才切 Server）。
        // 若 AutoNAT 未确认或处于 NAT 后，节点会停留在 Client 模式，
        // 不响应 DHT 查询，导致 put_record 等操作因 QuorumFailed 失败。
        // 在已知可达的场景（如测试、引导节点）可强制设为 Server。
        let lan_helper = match config.infrastructure_mode {
            InfrastructureMode::LanHelper(config) => Some(config),
            InfrastructureMode::Off => None,
        };
        let kad_server_mode =
            config.kad_server_mode || lan_helper.is_some_and(|c| c.enable_kad_server);
        if kad_server_mode {
            kad.set_mode(Some(kad::Mode::Server));
        }

        // ===== mDNS =====
        // 局域网多播 DNS 发现
        // 自动发现同一局域网内的其他节点，无需引导节点
        let mdns = Toggle::from(if config.enable_mdns {
            Some(
                mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
                    .expect("mDNS initialization failed"),
            )
        } else {
            None
        });

        // ===== AutoNAT v2 Client =====
        // 定期向已连接的 AutoNAT v2 Server（如引导节点）发送探测请求，
        // 让对方回拨自身地址以确认外部可达性。
        // 成功确认的地址会自动注册为 ExternalAddr。
        let autonat = Toggle::from(if config.enable_autonat {
            Some(autonat::v2::client::Behaviour::default())
        } else {
            None
        });

        // ===== DCUtR =====
        // Direct Connection Upgrade through Relay
        // 通过中继连接协调打洞，实现 NAT 穿透后的直连
        let dcutr = Toggle::from(if config.enable_dcutr {
            Some(dcutr::Behaviour::new(peer_id))
        } else {
            None
        });

        // ===== Relay Server =====
        // relay::Behaviour 是服务端；relay::client::Behaviour 是客户端。
        // LAN Helper 可以同时作为 relay client 和 relay server，但两个语义必须分开。
        let relay_server = Toggle::from(
            lan_helper.map(|helper| relay::Behaviour::new(peer_id, relay_config(helper))),
        );

        let req_resp = request_response::cbor::Behaviour::new(
            [(
                StreamProtocol::try_from_owned(config.req_resp_protocol.clone())
                    .expect("invalid req_resp_protocol"),
                request_response::ProtocolSupport::Full,
            )],
            request_response::Config::default().with_request_timeout(config.req_resp_timeout),
        );

        // ===== Data Channel (libp2p-stream) =====
        // 通用应用自定义字节流。Behaviour 不通过 SwarmEvent 交互，
        // 而是由运行时持有 Control + IncomingStreams（见 node.rs / event_loop.rs）。
        let stream = libp2p_stream::Behaviour::new();

        Self {
            ping,
            identify,
            kad,
            mdns,
            relay_client: Toggle::from(config.enable_relay_client.then_some(relay_client)),
            relay_server,
            autonat,
            dcutr,
            req_resp,
            stream,
        }
    }
}

fn relay_config(helper: LanHelperConfig) -> relay::Config {
    let RelayLimits {
        max_reservations,
        max_reservations_per_peer,
        reservation_duration,
        max_circuits,
        max_circuits_per_peer,
        max_circuit_duration,
        max_circuit_bytes,
    } = helper.relay_limits;

    relay::Config {
        max_reservations,
        max_reservations_per_peer,
        reservation_duration,
        max_circuits,
        max_circuits_per_peer,
        max_circuit_duration,
        max_circuit_bytes,
        ..Default::default()
    }
}
