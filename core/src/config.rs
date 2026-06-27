use std::time::Duration;

use libp2p::{Multiaddr, PeerId, StreamProtocol};

use crate::data_channel::DataChannelLimits;

/// Relay Server 资源限额。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelayLimits {
    pub max_reservations: usize,
    pub max_reservations_per_peer: usize,
    pub reservation_duration: Duration,
    pub max_circuits: usize,
    pub max_circuits_per_peer: usize,
    pub max_circuit_duration: Duration,
    pub max_circuit_bytes: u64,
}

impl Default for RelayLimits {
    fn default() -> Self {
        Self {
            max_reservations: 16,
            max_reservations_per_peer: 2,
            reservation_duration: Duration::from_secs(30 * 60),
            max_circuits: 8,
            max_circuits_per_peer: 2,
            max_circuit_duration: Duration::from_secs(30 * 60),
            max_circuit_bytes: 64 * 1024 * 1024,
        }
    }
}

/// 本节点是否提供基础设施能力。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum InfrastructureMode {
    /// 普通客户端，不提供 Kad/Relay 服务端能力。
    #[default]
    Off,
    /// 局域网协助节点：为同网段设备提供受限 Kad Server + Relay Server。
    LanHelper(LanHelperConfig),
}

/// 局域网协助节点配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LanHelperConfig {
    /// 是否强制 Kad 以 Server 模式运行。
    pub enable_kad_server: bool,
    /// Relay Server 资源限额。
    pub relay_limits: RelayLimits,
    /// 是否把可用私有 LAN 地址注册为可公告地址，供 relay reservation 返回。
    pub announce_private_addrs: bool,
}

impl Default for LanHelperConfig {
    fn default() -> Self {
        Self {
            enable_kad_server: true,
            relay_limits: RelayLimits::default(),
            announce_private_addrs: true,
        }
    }
}

/// infrastructure peer 的能力角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct InfrastructureRoles {
    pub kad_server: bool,
    pub relay_server: bool,
}

impl InfrastructureRoles {
    pub fn kad_server() -> Self {
        Self {
            kad_server: true,
            relay_server: false,
        }
    }

    pub fn relay_server() -> Self {
        Self {
            kad_server: false,
            relay_server: true,
        }
    }

    pub fn kad_and_relay() -> Self {
        Self {
            kad_server: true,
            relay_server: true,
        }
    }
}

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

    /// 本节点提供的基础设施能力。
    pub infrastructure_mode: InfrastructureMode,

    /// Request-Response 协议名称（如 "/myapp/req/1.0.0"）
    pub req_resp_protocol: String,

    /// Request-Response 请求超时时间
    ///
    /// 配对等需要用户交互的场景，默认 10 秒太短，建议 120 秒。
    pub req_resp_timeout: Duration,

    /// 注册的 data-channel 协议（应用自定义字节流协议，如 `/swarmdrop/transfer-data/1`）。
    ///
    /// 空表示不接受任何 data-channel 入站流。
    pub data_channel_protocols: Vec<StreamProtocol>,

    /// data-channel 出站打开超时。
    pub data_channel_open_timeout: Duration,

    /// data-channel 空闲超时（保留，供未来空闲回收用）。
    pub data_channel_idle_timeout: Duration,

    /// data-channel 活跃通道数量限制（per-peer / per-protocol）。
    pub data_channel_limits: DataChannelLimits,
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
            infrastructure_mode: InfrastructureMode::Off,
            req_resp_protocol: "/swarm-p2p/req/1.0.0".into(),
            req_resp_timeout: Duration::from_secs(120),
            data_channel_protocols: vec![],
            data_channel_open_timeout: Duration::from_secs(30),
            data_channel_idle_timeout: Duration::from_secs(300),
            data_channel_limits: DataChannelLimits::default(),
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

    pub fn with_infrastructure_mode(mut self, mode: InfrastructureMode) -> Self {
        self.infrastructure_mode = mode;
        self
    }

    pub fn with_lan_helper(mut self, config: LanHelperConfig) -> Self {
        self.infrastructure_mode = InfrastructureMode::LanHelper(config);
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

    /// 注册 data-channel 协议。
    pub fn with_data_channel_protocols(mut self, protocols: Vec<StreamProtocol>) -> Self {
        self.data_channel_protocols = protocols;
        self
    }

    /// 设置 data-channel 出站打开超时。
    pub fn with_data_channel_open_timeout(mut self, timeout: Duration) -> Self {
        self.data_channel_open_timeout = timeout;
        self
    }

    /// 设置 data-channel 空闲超时。
    pub fn with_data_channel_idle_timeout(mut self, timeout: Duration) -> Self {
        self.data_channel_idle_timeout = timeout;
        self
    }

    /// 设置 data-channel 数量限制。
    pub fn with_data_channel_limits(mut self, limits: DataChannelLimits) -> Self {
        self.data_channel_limits = limits;
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
        assert!(!config.kad_server_mode);
        assert_eq!(config.infrastructure_mode, InfrastructureMode::Off);
        assert_eq!(config.req_resp_protocol, "/swarm-p2p/req/1.0.0");
        assert_eq!(config.req_resp_timeout, Duration::from_secs(120));
        assert!(config.data_channel_protocols.is_empty());
        assert_eq!(config.data_channel_open_timeout, Duration::from_secs(30));
        assert_eq!(config.data_channel_idle_timeout, Duration::from_secs(300));
        assert_eq!(config.data_channel_limits.max_inbound_per_peer, 4);
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
        let data_protocol = StreamProtocol::new("/test/data/1");
        let limits = DataChannelLimits {
            max_inbound_per_peer: 1,
            max_outbound_per_peer: 2,
            max_per_protocol: 3,
        };
        let lan_helper = LanHelperConfig {
            enable_kad_server: false,
            relay_limits: RelayLimits {
                max_reservations: 4,
                max_reservations_per_peer: 1,
                reservation_duration: Duration::from_secs(60),
                max_circuits: 2,
                max_circuits_per_peer: 1,
                max_circuit_duration: Duration::from_secs(120),
                max_circuit_bytes: 1024,
            },
            announce_private_addrs: false,
        };
        let config = NodeConfig::new("/test/1.0.0", "Test/1.0.0")
            .with_listen_addrs(vec![addr.clone()])
            .with_mdns(false)
            .with_relay_client(false)
            .with_dcutr(false)
            .with_autonat(false)
            .with_lan_helper(lan_helper)
            .with_req_resp_protocol("/test/req/1.0.0")
            .with_req_resp_timeout(Duration::from_secs(7))
            .with_data_channel_protocols(vec![data_protocol.clone()])
            .with_data_channel_open_timeout(Duration::from_secs(8))
            .with_data_channel_idle_timeout(Duration::from_secs(9))
            .with_data_channel_limits(limits);

        assert_eq!(config.listen_addrs, vec![addr]);
        assert!(!config.enable_mdns);
        assert!(!config.enable_relay_client);
        assert!(!config.enable_dcutr);
        assert!(!config.enable_autonat);
        assert_eq!(
            config.infrastructure_mode,
            InfrastructureMode::LanHelper(lan_helper)
        );
        assert_eq!(config.req_resp_protocol, "/test/req/1.0.0");
        assert_eq!(config.req_resp_timeout, Duration::from_secs(7));
        assert_eq!(config.data_channel_protocols, vec![data_protocol]);
        assert_eq!(config.data_channel_open_timeout, Duration::from_secs(8));
        assert_eq!(config.data_channel_idle_timeout, Duration::from_secs(9));
        assert_eq!(config.data_channel_limits.max_inbound_per_peer, 1);
        assert_eq!(config.data_channel_limits.max_outbound_per_peer, 2);
        assert_eq!(config.data_channel_limits.max_per_protocol, 3);
    }

    #[test]
    fn clone_is_independent() {
        let config = NodeConfig::default();
        let mut config2 = config.clone();
        config2.enable_mdns = false;
        assert!(config.enable_mdns);
        assert!(!config2.enable_mdns);
    }

    #[test]
    fn lan_helper_defaults_are_conservative() {
        let config = LanHelperConfig::default();

        assert!(config.enable_kad_server);
        assert!(config.announce_private_addrs);
        assert_eq!(config.relay_limits.max_reservations, 16);
        assert_eq!(config.relay_limits.max_reservations_per_peer, 2);
        assert_eq!(config.relay_limits.max_circuits, 8);
        assert_eq!(config.relay_limits.max_circuits_per_peer, 2);
        assert_eq!(config.relay_limits.max_circuit_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn infrastructure_roles_helpers() {
        assert_eq!(
            InfrastructureRoles::kad_server(),
            InfrastructureRoles {
                kad_server: true,
                relay_server: false
            }
        );
        assert_eq!(
            InfrastructureRoles::relay_server(),
            InfrastructureRoles {
                kad_server: false,
                relay_server: true
            }
        );
        assert_eq!(
            InfrastructureRoles::kad_and_relay(),
            InfrastructureRoles {
                kad_server: true,
                relay_server: true
            }
        );
    }
}
