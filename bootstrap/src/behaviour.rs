use std::num::NonZeroUsize;
use std::time::Duration;

use libp2p::{autonat, identify, identity::Keypair, kad, ping, relay, swarm::NetworkBehaviour};

/// 引导+中继节点的轻量网络行为
///
/// 只包含服务端必需的协议：
/// - `ping`: 心跳保活
/// - `identify`: 节点信息交换（客户端通过 identify 获取引导节点的监听地址）
/// - `kad`: Kademlia DHT Server 模式，响应所有 DHT 查询
/// - `relay`: Relay Server，为 NAT 后的节点中继流量
/// - `autonat`: AutoNAT v2 Server，响应客户端的 NAT 检测请求
#[derive(NetworkBehaviour)]
pub struct BootstrapBehaviour {
    pub ping: ping::Behaviour,
    pub identify: identify::Behaviour,
    pub kad: kad::Behaviour<kad::store::MemoryStore>,
    pub relay: relay::Behaviour,
    pub autonat: autonat::v2::server::Behaviour,
}

impl BootstrapBehaviour {
    pub fn new(keypair: &Keypair) -> Self {
        let peer_id = keypair.public().to_peer_id();

        // ===== Ping =====
        let ping = ping::Behaviour::new(
            ping::Config::new()
                .with_interval(Duration::from_secs(15))
                .with_timeout(Duration::from_secs(10)),
        );

        // ===== Identify =====
        // protocol_version 必须与客户端一致（"/swarmdrop/1.0.0"），
        // 客户端 event_loop 只在 protocol_version 匹配时才将对方加入 Kad 路由表。
        let identify = identify::Behaviour::new(
            identify::Config::new("/swarmdrop/1.0.0".to_string(), keypair.public())
                .with_agent_version(format!("swarm-bootstrap/{}", env!("CARGO_PKG_VERSION")))
                .with_push_listen_addr_updates(true)
                .with_cache_size(1000),
        );

        // ===== Kademlia DHT =====
        // 强制 Server 模式：始终响应 DHT 查询
        // 作为引导节点，record_ttl 和 replication_factor 适当放大
        let mut kad_config = kad::Config::default();
        kad_config
            .set_query_timeout(Duration::from_secs(60))
            .set_record_ttl(Some(Duration::from_secs(7200))) // 2 小时
            .set_replication_factor(NonZeroUsize::new(20).unwrap())
            .set_publication_interval(Some(Duration::from_secs(3600)))
            .set_provider_record_ttl(Some(Duration::from_secs(7200)));

        let mut kad =
            kad::Behaviour::with_config(peer_id, kad::store::MemoryStore::new(peer_id), kad_config);
        kad.set_mode(Some(kad::Mode::Server));

        // ===== Relay Server =====
        // 为 NAT 后的节点提供中继服务
        // relay::Behaviour 是服务端，与客户端的 relay::client::Behaviour 不同
        //
        // 默认限制过于严格（128KB / 2min），文件传输会被切断。
        // 放大限制以支持大文件传输（理想情况下 DCUtR 打洞成功后会走直连，
        // relay 只在打洞失败时作为兜底）。
        let relay_config = relay::Config {
            max_circuit_bytes: 1024 * 1024 * 512,            // 512 MB
            max_circuit_duration: Duration::from_secs(3600), // 1 小时
            ..Default::default()
        };
        let relay = relay::Behaviour::new(peer_id, relay_config);

        // ===== AutoNAT v2 Server =====
        // 为客户端提供 NAT 检测服务：客户端请求引导节点回拨其地址，
        // 以此判断客户端是否公网可达。
        let autonat = autonat::v2::server::Behaviour::default();

        Self {
            ping,
            identify,
            kad,
            relay,
            autonat,
        }
    }
}
