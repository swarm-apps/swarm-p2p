use std::time::Duration;

use libp2p::{Multiaddr, PeerId};

/// 节点配置
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// identify 协议版本（如 "/myapp/1.0.0"）
    pub protocol_version: String,

    /// identify agent 版本（如 "myapp/1.0.0;os=macos"）
    pub agent_version: String,

    /// 监听地址
    pub listen_addrs: Vec<Multiaddr>,

    /// Kademlia DHT 引导节点
    pub bootstrap_peers: Vec<(PeerId, Multiaddr)>,

    /// 启用 mDNS 局域网发现
    pub enable_mdns: bool,

    /// 启用 relay 中继客户端（NAT 穿透）
    pub enable_relay_client: bool,

    /// 启用 DCUtR 打洞
    pub enable_dcutr: bool,

    /// 启用 AutoNAT 检测
    pub enable_autonat: bool,

    /// 空闲连接超时时间
    pub idle_connection_timeout: Duration,

    /// Ping 间隔
    pub ping_interval: Duration,

    /// Ping 超时
    pub ping_timeout: Duration,

    /// Kademlia 查询超时
    pub kad_query_timeout: Duration,

    /// 强制 Kad 以 Server 模式运行
    ///
    /// 默认 `false`（自动模式，由 AutoNAT 决定）。
    /// 设为 `true` 后节点始终响应 DHT 查询，适用于确认公网可达或测试场景。
    pub kad_server_mode: bool,

    /// Request-Response 协议名称（如 "/myapp/req/1.0.0"）
    pub req_resp_protocol: String,

    /// Request-Response 请求超时时间
    ///
    /// 配对等需要用户交互的场景，默认 10 秒太短，建议 120 秒。
    pub req_resp_timeout: Duration,

    /// 启用 GossipSub pub/sub 协议
    pub enable_gossipsub: bool,

    /// GossipSub 心跳间隔
    pub gossipsub_heartbeat_interval: Duration,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            protocol_version: "/swarm-p2p/1.0.0".into(),
            agent_version: format!("swarm-p2p/{}", env!("CARGO_PKG_VERSION")),
            listen_addrs: vec![
                "/ip4/0.0.0.0/tcp/0".parse().unwrap(),
                "/ip6/::/tcp/0".parse().unwrap(),
            ],
            bootstrap_peers: vec![],
            enable_mdns: true,
            enable_relay_client: true,
            enable_dcutr: true,
            enable_autonat: true,
            idle_connection_timeout: Duration::from_secs(60),
            ping_interval: Duration::from_secs(15),
            ping_timeout: Duration::from_secs(10),
            kad_query_timeout: Duration::from_secs(60),
            kad_server_mode: false,
            req_resp_protocol: "/swarm-p2p/req/1.0.0".into(),
            req_resp_timeout: Duration::from_secs(120),
            enable_gossipsub: true,
            gossipsub_heartbeat_interval: Duration::from_secs(10),
        }
    }
}

impl NodeConfig {
    pub fn new(protocol_version: impl Into<String>, agent_version: impl Into<String>) -> Self {
        Self {
            protocol_version: protocol_version.into(),
            agent_version: agent_version.into(),
            ..Default::default()
        }
    }

    pub fn with_listen_addrs(mut self, addrs: Vec<Multiaddr>) -> Self {
        self.listen_addrs = addrs;
        self
    }

    pub fn with_bootstrap_peers(mut self, peers: Vec<(PeerId, Multiaddr)>) -> Self {
        self.bootstrap_peers = peers;
        self
    }

    pub fn with_mdns(mut self, enable: bool) -> Self {
        self.enable_mdns = enable;
        self
    }

    pub fn with_relay_client(mut self, enable: bool) -> Self {
        self.enable_relay_client = enable;
        self
    }

    pub fn with_dcutr(mut self, enable: bool) -> Self {
        self.enable_dcutr = enable;
        self
    }

    pub fn with_autonat(mut self, enable: bool) -> Self {
        self.enable_autonat = enable;
        self
    }

    pub fn with_kad_server_mode(mut self, enable: bool) -> Self {
        self.kad_server_mode = enable;
        self
    }

    pub fn with_req_resp_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.req_resp_protocol = protocol.into();
        self
    }

    pub fn with_req_resp_timeout(mut self, timeout: Duration) -> Self {
        self.req_resp_timeout = timeout;
        self
    }

    pub fn with_gossipsub(mut self, enable: bool) -> Self {
        self.enable_gossipsub = enable;
        self
    }

    pub fn with_gossipsub_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.gossipsub_heartbeat_interval = interval;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values() {
        let config = NodeConfig::default();
        assert_eq!(config.protocol_version, "/swarm-p2p/1.0.0");
        assert!(config.agent_version.starts_with("swarm-p2p/"));
        assert_eq!(config.listen_addrs.len(), 2);
        assert!(config.bootstrap_peers.is_empty());
        assert!(config.enable_mdns);
        assert!(config.enable_relay_client);
        assert!(config.enable_dcutr);
        assert!(config.enable_autonat);
        assert_eq!(config.idle_connection_timeout, Duration::from_secs(60));
        assert_eq!(config.ping_interval, Duration::from_secs(15));
        assert_eq!(config.ping_timeout, Duration::from_secs(10));
        assert_eq!(config.kad_query_timeout, Duration::from_secs(60));
        assert_eq!(config.req_resp_protocol, "/swarm-p2p/req/1.0.0");
        assert_eq!(config.req_resp_timeout, Duration::from_secs(120));
        assert!(config.enable_gossipsub);
        assert_eq!(config.gossipsub_heartbeat_interval, Duration::from_secs(10));
    }

    #[test]
    fn new_overrides_protocol_and_agent() {
        let config = NodeConfig::new("/myapp/2.0.0", "MyApp/2.0.0");
        assert_eq!(config.protocol_version, "/myapp/2.0.0");
        assert_eq!(config.agent_version, "MyApp/2.0.0");
        // 其余字段保持默认
        assert!(config.enable_mdns);
    }

    #[test]
    fn builder_chain() {
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();
        let config = NodeConfig::new("/test/1.0.0", "Test/1.0.0")
            .with_listen_addrs(vec![addr.clone()])
            .with_mdns(false)
            .with_relay_client(false)
            .with_dcutr(false)
            .with_autonat(false)
            .with_req_resp_protocol("/test/req/1.0.0");

        assert_eq!(config.listen_addrs, vec![addr]);
        assert!(!config.enable_mdns);
        assert!(!config.enable_relay_client);
        assert!(!config.enable_dcutr);
        assert!(!config.enable_autonat);
        assert_eq!(config.req_resp_protocol, "/test/req/1.0.0");
    }

    #[test]
    fn clone_is_independent() {
        let config = NodeConfig::default();
        let mut config2 = config.clone();
        config2.enable_mdns = false;
        assert!(config.enable_mdns);
        assert!(!config2.enable_mdns);
    }
}
