use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::StreamExt;
use futures::stream::{BoxStream, SelectAll};
use libp2p::request_response::{Event as ReqRespEvent, Message};
use libp2p::swarm::SwarmEvent;
use libp2p::{PeerId, Stream, StreamProtocol, autonat, dcutr, ping};
use libp2p_stream::IncomingStreams;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::{CborMessage, CoreBehaviourEvent};
use crate::command::{Command, CoreSwarm};
use crate::config::{InfrastructureMode, LanHelperConfig};
use crate::data_channel::{
    ChannelRegistry, DataChannel, DataChannelDirection, DataChannelId, InboundDataChannel,
};
use crate::event::{NatStatus, NodeEvent};
use crate::pending_map::PendingMap;

/// 事件循环
pub struct EventLoop<Req, Resp>
where
    Req: CborMessage,
    Resp: CborMessage,
{
    swarm: CoreSwarm<Req, Resp>,
    command_rx: mpsc::Receiver<Command<Req, Resp>>,
    event_tx: mpsc::Sender<NodeEvent<Req>>,
    active_commands: Vec<Command<Req, Resp>>,
    /// 本机的协议版本，用于判断是否加入 Kad
    protocol_version: String,
    /// 暂存 inbound request 的 ResponseChannel，等待前端回复
    pending_channels: PendingMap<u64, libp2p::request_response::ResponseChannel<Resp>>,
    /// pending_id 自增计数器
    pending_id_counter: AtomicU64,
    /// Bootstrap 节点地址映射（peer_id → 地址列表），
    /// 用于在连接建立后申请 relay reservation
    bootstrap_peers: HashMap<libp2p::PeerId, Vec<libp2p::Multiaddr>>,
    /// 是否在连接 bootstrap 后申请 relay reservation。
    enable_relay_client: bool,
    /// LAN Helper 配置；为空表示普通客户端模式。
    lan_helper: Option<LanHelperConfig>,
    /// 已注册为 external address 的 LAN Helper 地址。
    advertised_lan_addrs: Vec<libp2p::Multiaddr>,
    /// 入站数据通道流（多协议合并），在 `select!` 中持续 poll；
    /// 空集合时该分支被守卫跳过，不会 busy-loop。
    inbound_channels: SelectAll<BoxStream<'static, (StreamProtocol, PeerId, Stream)>>,
    /// 数据通道配额登记表（入站 limit 校验）。
    dc_registry: ChannelRegistry,
    /// 入站数据通道转交端（非阻塞 try_send 给运行时消费者）。
    inbound_dc_tx: mpsc::Sender<InboundDataChannel>,
}

impl<Req, Resp> EventLoop<Req, Resp>
where
    Req: CborMessage,
    Resp: CborMessage,
{
    #[expect(
        clippy::too_many_arguments,
        reason = "事件循环装配需要完整运行时上下文"
    )]
    pub(crate) fn new(
        swarm: CoreSwarm<Req, Resp>,
        command_rx: mpsc::Receiver<Command<Req, Resp>>,
        event_tx: mpsc::Sender<NodeEvent<Req>>,
        pending_channels: PendingMap<u64, libp2p::request_response::ResponseChannel<Resp>>,
        protocol_version: String,
        inbound_protocol_streams: Vec<(StreamProtocol, IncomingStreams)>,
        dc_registry: ChannelRegistry,
        inbound_dc_tx: mpsc::Sender<InboundDataChannel>,
        enable_relay_client: bool,
        infrastructure_mode: InfrastructureMode,
    ) -> Self {
        // 把每个协议的入站流贴上 protocol 标签后合并，统一在 select! 中 poll。
        let inbound_channels =
            futures::stream::select_all(inbound_protocol_streams.into_iter().map(
                |(proto, incoming)| {
                    incoming
                        .map(move |(peer, stream)| (proto.clone(), peer, stream))
                        .boxed()
                },
            ));
        Self {
            swarm,
            command_rx,
            event_tx,
            active_commands: Vec::new(),
            protocol_version,
            pending_channels,
            pending_id_counter: AtomicU64::new(0),
            bootstrap_peers: HashMap::new(),
            enable_relay_client,
            lan_helper: match infrastructure_mode {
                InfrastructureMode::LanHelper(config) => Some(config),
                InfrastructureMode::Off => None,
            },
            advertised_lan_addrs: Vec::new(),
            inbound_channels,
            dc_registry,
            inbound_dc_tx,
        }
    }

    /// 启动监听
    pub fn start_listen(&mut self, addrs: &[libp2p::Multiaddr]) -> crate::Result<()> {
        for addr in addrs {
            self.swarm
                .listen_on(addr.clone())
                .map_err(|e| crate::error::Error::Listen(e.to_string()))?;
        }
        Ok(())
    }

    /// 连接引导节点：注册地址到 Kad 路由表、dial，并记录 bootstrap 节点用于后续 relay reservation
    pub fn connect_bootstrap_peers(&mut self, peers: &[(libp2p::PeerId, libp2p::Multiaddr)]) {
        for (peer_id, addr) in peers {
            self.swarm
                .behaviour_mut()
                .kad
                .add_address(peer_id, addr.clone());
            self.swarm.add_peer_address(*peer_id, addr.clone());
            if let Err(e) = self.swarm.dial(*peer_id) {
                warn!("Failed to dial bootstrap peer {}: {}", peer_id, e);
            } else {
                info!("Dialing bootstrap peer {} at {}", peer_id, addr);
            }

            if self.enable_relay_client {
                self.bootstrap_peers
                    .entry(*peer_id)
                    .or_default()
                    .push(addr.clone());
            }
        }
    }

    /// 运行事件循环
    pub async fn run(mut self) {
        loop {
            // 清理调用方已 drop / 取消的 active command（避免泄漏到全局 timeout）
            self.active_commands.retain(|cmd| !cmd.is_cancelled());
            tokio::select! {
                // 处理外部命令
                cmd = self.command_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd).await,
                        None => {
                            info!("Command channel closed, shutting down");
                            return;
                        }
                    }
                }
                // 处理 swarm 事件
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                }
                // 处理入站数据通道（空集合时跳过该分支，避免 busy-loop）
                maybe_inbound = self.inbound_channels.next(), if !self.inbound_channels.is_empty() => {
                    if let Some((protocol, peer, stream)) = maybe_inbound {
                        self.handle_inbound_channel(protocol, peer, stream);
                    }
                }
            }
        }
    }

    /// 接受入站数据通道：校验 limit、生成 handle、非阻塞转交给消费者。
    ///
    /// 绝不阻塞 swarm 循环——转交用 `try_send`，消费者落后时丢弃并告警，
    /// 而非反压拖死 ping / kad / identify。
    fn handle_inbound_channel(&mut self, protocol: StreamProtocol, peer: PeerId, stream: Stream) {
        let Some(guard) =
            self.dc_registry
                .try_acquire(peer, protocol.clone(), DataChannelDirection::Inbound)
        else {
            warn!(
                "入站数据通道被拒绝（超出 limit）: peer={}, protocol={}",
                peer, protocol
            );
            drop(stream);
            return;
        };
        let id = DataChannelId::next();
        let channel = DataChannel::new(
            id,
            peer,
            protocol,
            DataChannelDirection::Inbound,
            stream,
            Some(guard),
        );
        match self.inbound_dc_tx.try_send(InboundDataChannel { channel }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("入站数据通道丢弃：消费者落后（channel 满），peer={}", peer)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!("入站数据通道丢弃：消费者已关闭")
            }
        }
    }

    async fn handle_command(&mut self, mut cmd: Command<Req, Resp>) {
        cmd.run_boxed(&mut self.swarm).await;
        if let Some((peer_id, addrs)) = cmd.take_relay_reservation_request() {
            if self.swarm.is_connected(&peer_id) {
                self.request_relay_reservations(peer_id, addrs);
            } else {
                self.bootstrap_peers
                    .entry(peer_id)
                    .or_default()
                    .extend(addrs);
            }
        }
        self.active_commands.push(cmd);
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<CoreBehaviourEvent<Req, Resp>>) {
        // 命令链：依次传递 owned event，命令可选择消费或传递
        let mut remaining = Some(event);
        let mut i = 0;
        while i < self.active_commands.len() {
            let Some(event) = remaining.take() else {
                break; // 事件已被消费，后续命令不再处理
            };
            let (keep, returned) = self.active_commands[i]
                .on_event_boxed(event, &mut self.swarm)
                .await;
            remaining = returned;
            if keep {
                i += 1;
            } else {
                self.active_commands.swap_remove(i);
            }
        }

        // 未被命令消费的事件，转换为前端事件
        let Some(event) = remaining else {
            return;
        };

        if let Some(evt) = self.convert_to_node_event(event) {
            let _ = self.event_tx.send(evt).await;
        }
    }

    fn next_pending_id(&self) -> u64 {
        self.pending_id_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// 将 swarm 事件转换为对外事件
    fn convert_to_node_event(
        &mut self,
        event: SwarmEvent<CoreBehaviourEvent<Req, Resp>>,
    ) -> Option<NodeEvent<Req>> {
        match event {
            SwarmEvent::Behaviour(CoreBehaviourEvent::RelayClient(e)) => match e {
                libp2p::relay::client::Event::ReservationReqAccepted {
                    relay_peer_id,
                    renewal,
                    ..
                } => {
                    info!(
                        "Relay reservation {} by {}",
                        if renewal { "renewed" } else { "accepted" },
                        relay_peer_id
                    );
                    Some(NodeEvent::RelayReservationAccepted {
                        relay_peer_id,
                        renewal,
                    })
                }
                libp2p::relay::client::Event::OutboundCircuitEstablished {
                    relay_peer_id, ..
                } => {
                    info!("Outbound circuit established via relay {}", relay_peer_id);
                    None
                }
                libp2p::relay::client::Event::InboundCircuitEstablished { src_peer_id, .. } => {
                    info!("Inbound circuit established from {}", src_peer_id);
                    None
                }
            },
            SwarmEvent::Behaviour(CoreBehaviourEvent::RelayServer(e)) => {
                self.convert_relay_server_event(e)
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                self.maybe_announce_lan_helper_addr(&address);
                Some(NodeEvent::Listening { addr: address })
            }
            // 只在第一个连接建立时通知（peer 级别聚合）
            SwarmEvent::ConnectionEstablished {
                peer_id,
                num_established,
                ..
            } if num_established.get() == 1 => Some(NodeEvent::PeerConnected { peer_id }),
            SwarmEvent::ConnectionEstablished { .. } => None,
            // 只在最后一个连接关闭时通知（peer 级别聚合）
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established: 0,
                ..
            } => Some(NodeEvent::PeerDisconnected { peer_id }),
            // Inbound request: 取出 ResponseChannel 暂存，通知前端
            SwarmEvent::Behaviour(CoreBehaviourEvent::ReqResp(ReqRespEvent::Message {
                peer,
                message:
                    Message::Request {
                        request, channel, ..
                    },
                ..
            })) => {
                let pending_id = self.next_pending_id();
                info!(
                    "Inbound request from {}, assigned pending_id={}",
                    peer, pending_id
                );
                self.pending_channels.insert(pending_id, channel);
                Some(NodeEvent::InboundRequest {
                    peer_id: peer,
                    pending_id,
                    request,
                })
            }
            SwarmEvent::Behaviour(CoreBehaviourEvent::Dcutr(dcutr::Event {
                remote_peer_id,
                result,
            })) => match result {
                Ok(_connection_id) => {
                    info!("DCUtR hole-punch succeeded with {}", remote_peer_id);
                    Some(NodeEvent::HolePunchSucceeded {
                        peer_id: remote_peer_id,
                    })
                }
                Err(e) => {
                    warn!("DCUtR hole-punch failed with {}: {}", remote_peer_id, e);
                    Some(NodeEvent::HolePunchFailed {
                        peer_id: remote_peer_id,
                        error: e.to_string(),
                    })
                }
            },
            SwarmEvent::Behaviour(CoreBehaviourEvent::Mdns(libp2p::mdns::Event::Discovered(
                peers,
            ))) => {
                // 先注册所有地址，再 dial（dial by PeerId 会使用所有已知地址）
                for (peer_id, addr) in &peers {
                    self.swarm.add_peer_address(*peer_id, addr.clone());
                }

                let dialed: std::collections::HashSet<_> =
                    peers.iter().map(|(id, _)| *id).collect();

                for peer_id in &dialed {
                    if !self.swarm.is_connected(peer_id) {
                        info!("mDNS: dialing peer {}", peer_id);
                        if let Err(e) = self.swarm.dial(*peer_id) {
                            warn!("Failed to dial discovered peer {}: {}", peer_id, e);
                        }
                    }
                }
                Some(NodeEvent::PeersDiscovered { peers })
            }
            SwarmEvent::Behaviour(CoreBehaviourEvent::Ping(ping::Event {
                peer,
                result: Ok(rtt),
                ..
            })) => Some(NodeEvent::PingSuccess {
                peer_id: peer,
                rtt_ms: rtt.as_millis() as u64,
            }),
            SwarmEvent::Behaviour(CoreBehaviourEvent::Identify(
                libp2p::identify::Event::Received { peer_id, info, .. },
            )) => {
                // 如果协议版本匹配，自动加入 Kad 并注册地址到 Swarm
                if info.protocol_version == self.protocol_version {
                    for addr in &info.listen_addrs {
                        self.swarm
                            .behaviour_mut()
                            .kad
                            .add_address(&peer_id, addr.clone());
                        self.swarm.add_peer_address(peer_id, addr.clone());
                    }
                    info!(
                        "Added peer {} to Kad + Swarm (protocol: {})",
                        peer_id, info.protocol_version
                    );
                    if let Some(addrs) = self.bootstrap_peers.remove(&peer_id) {
                        self.request_relay_reservations(peer_id, addrs);
                    }
                } else {
                    debug!(
                        "Peer {} protocol mismatch: expected {}, got {}",
                        peer_id, self.protocol_version, info.protocol_version
                    );
                }
                Some(NodeEvent::IdentifyReceived {
                    peer_id,
                    agent_version: info.agent_version,
                    protocol_version: info.protocol_version,
                    listen_addrs: info.listen_addrs,
                    protocols: info.protocols.into_iter().map(|p| p.to_string()).collect(),
                })
            }
            // AutoNAT: 仅在探测成功时上报 Public 状态。
            // 单次探测失败不代表节点在 NAT 后面（可能是探测服务器自身不可达），
            // 因此失败时保持 Unknown，避免误判为 Private。
            SwarmEvent::Behaviour(CoreBehaviourEvent::Autonat(autonat::v2::client::Event {
                tested_addr,
                server,
                result,
                ..
            })) => match result {
                Ok(()) => {
                    info!(
                        "AutoNAT: address {} confirmed reachable by {}",
                        tested_addr, server
                    );
                    Some(NodeEvent::NatStatusChanged {
                        status: NatStatus::Public,
                        public_addr: Some(tested_addr),
                    })
                }
                Err(e) => {
                    debug!(
                        "AutoNAT: address {} not reachable via {}: {}",
                        tested_addr, server, e
                    );
                    None
                }
            },
            // Kad 路由表更新：将学到的地址同步到 Swarm 地址簿，
            // 确保后续 dial(peer_id) 能找到地址（跨网络 DHT 查询场景）
            SwarmEvent::Behaviour(CoreBehaviourEvent::Kad(
                libp2p::kad::Event::RoutingUpdated {
                    peer, addresses, ..
                },
            )) => {
                for addr in addresses.iter() {
                    self.swarm.add_peer_address(peer, addr.clone());
                }
                debug!(
                    "Kad routing updated for {}, synced {} addrs to swarm",
                    peer,
                    addresses.len()
                );
                None
            }
            SwarmEvent::ListenerClosed {
                listener_id,
                addresses,
                reason,
            } => {
                warn!(
                    "Listener {:?} closed (addresses: {:?}): {:?}",
                    listener_id, addresses, reason
                );
                None
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                warn!("Listener {:?} error: {}", listener_id, error);
                None
            }
            SwarmEvent::IncomingConnectionError {
                local_addr,
                send_back_addr,
                error,
                ..
            } => {
                debug!(
                    "Incoming connection error: local={}, remote={}, err={}",
                    local_addr, send_back_addr, error
                );
                None
            }
            _ => None,
        }
    }

    fn convert_relay_server_event(&self, event: libp2p::relay::Event) -> Option<NodeEvent<Req>> {
        match event {
            libp2p::relay::Event::ReservationReqAccepted {
                src_peer_id,
                renewed,
            } => {
                info!(
                    "Relay server accepted reservation from {} (renewed={})",
                    src_peer_id, renewed
                );
                Some(NodeEvent::RelayServerReservationAccepted {
                    src_peer_id,
                    renewed,
                })
            }
            libp2p::relay::Event::ReservationReqDenied {
                src_peer_id,
                status,
            } => {
                warn!(
                    "Relay server denied reservation from {}: {:?}",
                    src_peer_id, status
                );
                Some(NodeEvent::RelayServerReservationDenied {
                    src_peer_id,
                    status: format!("{status:?}"),
                })
            }
            libp2p::relay::Event::ReservationClosed { src_peer_id }
            | libp2p::relay::Event::ReservationTimedOut { src_peer_id } => {
                info!("Relay server reservation closed for {}", src_peer_id);
                Some(NodeEvent::RelayServerReservationClosed { src_peer_id })
            }
            libp2p::relay::Event::CircuitReqAccepted {
                src_peer_id,
                dst_peer_id,
            } => {
                info!(
                    "Relay server accepted circuit {} -> {}",
                    src_peer_id, dst_peer_id
                );
                Some(NodeEvent::RelayServerCircuitAccepted {
                    src_peer_id,
                    dst_peer_id,
                })
            }
            libp2p::relay::Event::CircuitReqDenied {
                src_peer_id,
                dst_peer_id,
                status,
            } => {
                warn!(
                    "Relay server denied circuit {} -> {}: {:?}",
                    src_peer_id, dst_peer_id, status
                );
                Some(NodeEvent::RelayServerCircuitDenied {
                    src_peer_id,
                    dst_peer_id,
                    status: format!("{status:?}"),
                })
            }
            libp2p::relay::Event::CircuitClosed {
                src_peer_id,
                dst_peer_id,
                ..
            } => {
                info!(
                    "Relay server circuit closed {} -> {}",
                    src_peer_id, dst_peer_id
                );
                Some(NodeEvent::RelayServerCircuitClosed {
                    src_peer_id,
                    dst_peer_id,
                })
            }
            #[allow(deprecated)]
            libp2p::relay::Event::ReservationReqAcceptFailed { .. }
            | libp2p::relay::Event::ReservationReqDenyFailed { .. }
            | libp2p::relay::Event::CircuitReqDenyFailed { .. }
            | libp2p::relay::Event::CircuitReqOutboundConnectFailed { .. }
            | libp2p::relay::Event::CircuitReqAcceptFailed { .. } => {
                debug!("Relay server transient failure: {:?}", event);
                None
            }
        }
    }

    fn maybe_announce_lan_helper_addr(&mut self, addr: &libp2p::Multiaddr) {
        let Some(config) = self.lan_helper else {
            return;
        };
        if !config.announce_private_addrs {
            return;
        }

        if is_usable_lan_addr(addr) {
            if !self.advertised_lan_addrs.contains(addr) {
                self.swarm.add_external_address(addr.clone());
                self.advertised_lan_addrs.push(addr.clone());
                info!("LAN Helper advertised address: {}", addr);
            }
        } else {
            debug!("LAN Helper ignored non-routable listen address: {}", addr);
        }

        let event = NodeEvent::LanHelperStatusChanged {
            relay_server_enabled: self.swarm.behaviour().relay_server.is_enabled(),
            advertised_addrs: self.advertised_lan_addrs.clone(),
        };
        if let Err(e) = self.event_tx.try_send(event) {
            debug!("LAN Helper status event dropped: {}", e);
        }
    }

    fn request_relay_reservations(
        &mut self,
        peer_id: libp2p::PeerId,
        addrs: Vec<libp2p::Multiaddr>,
    ) {
        for addr in addrs {
            let base = if addr
                .iter()
                .any(|p| matches!(p, libp2p::multiaddr::Protocol::P2p(_)))
            {
                addr.clone()
            } else {
                addr.clone().with(libp2p::multiaddr::Protocol::P2p(peer_id))
            };
            let relay_addr = base.with(libp2p::multiaddr::Protocol::P2pCircuit);
            match self.swarm.listen_on(relay_addr.clone()) {
                Ok(_) => info!("Requesting relay reservation via {}", relay_addr),
                Err(e) => warn!("Failed to listen on relay circuit {}: {}", relay_addr, e),
            }
        }
    }
}

fn is_usable_lan_addr(addr: &libp2p::Multiaddr) -> bool {
    addr.iter().any(|protocol| match protocol {
        libp2p::multiaddr::Protocol::Ip4(ip) => {
            ip.is_private() && !ip.is_loopback() && !ip.is_link_local() && !ip.is_unspecified()
        }
        libp2p::multiaddr::Protocol::Ip6(ip) => {
            (ip.segments()[0] & 0xfe00) == 0xfc00 && !ip.is_loopback() && !ip.is_unspecified()
        }
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::is_usable_lan_addr;
    use libp2p::Multiaddr;

    #[test]
    fn usable_lan_addr_filters_non_routable_addresses() {
        let private: Multiaddr = "/ip4/192.168.1.10/tcp/4001".parse().unwrap();
        let wildcard: Multiaddr = "/ip4/0.0.0.0/tcp/4001".parse().unwrap();
        let loopback: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();
        let link_local: Multiaddr = "/ip4/169.254.1.1/tcp/4001".parse().unwrap();
        let public: Multiaddr = "/ip4/8.8.8.8/tcp/4001".parse().unwrap();
        let unique_local: Multiaddr = "/ip6/fd00::1/tcp/4001".parse().unwrap();
        let ipv6_loopback: Multiaddr = "/ip6/::1/tcp/4001".parse().unwrap();

        assert!(is_usable_lan_addr(&private));
        assert!(is_usable_lan_addr(&unique_local));
        assert!(!is_usable_lan_addr(&wildcard));
        assert!(!is_usable_lan_addr(&loopback));
        assert!(!is_usable_lan_addr(&link_local));
        assert!(!is_usable_lan_addr(&public));
        assert!(!is_usable_lan_addr(&ipv6_loopback));
    }
}
