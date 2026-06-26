use anyhow::Result;
use libp2p::{SwarmBuilder, noise, tcp, yamux};
use tokio::sync::mpsc;
use tracing::warn;

use super::event_loop::EventLoop;
use super::{CborMessage, CoreBehaviour};
use crate::client::{EventReceiver, NetClient};
use crate::config::NodeConfig;
use crate::data_channel::{ChannelRegistry, DataChannelReceiver};
use crate::pending_map::PendingMap;

const COMMAND_CHANNEL_SIZE: usize = 32;
const EVENT_CHANNEL_SIZE: usize = 64;
const DC_INBOUND_CHANNEL_SIZE: usize = 16;

/// 启动节点
///
/// 返回 (NetClient, EventReceiver, DataChannelReceiver)：
/// - NetClient: 用于发送命令（dial / send_request / open_data_channel 等）
/// - EventReceiver: 用于接收节点事件（peer discovered, connected 等）
/// - DataChannelReceiver: 用于接收入站数据通道（非序列化 stream handle）
///
/// Transport 层包含：
/// - TCP + Noise + Yamux（稳定连接，防火墙友好）
/// - QUIC（内置 TLS 1.3 加密和多路复用，NAT 穿透更优）
/// - Relay client（无法直连时的兜底）
/// - DNS 解析（支持 /dnsaddr/, /dns4/, /dns6/ multiaddr）
pub fn start<Req, Resp>(
    keypair: libp2p::identity::Keypair,
    config: NodeConfig,
) -> Result<(
    NetClient<Req, Resp>,
    EventReceiver<Req>,
    DataChannelReceiver,
)>
where
    Req: CborMessage,
    Resp: CborMessage,
{
    // 构建 swarm：TCP + QUIC + (可选 DNS) + Relay
    // dns feature 由上层按平台决定是否启用（Android 上 /etc/resolv.conf 不存在）
    let builder = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic();

    #[cfg(feature = "dns")]
    let builder = builder.with_dns()?;

    let swarm = builder
        .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(|key, relay_client| {
            CoreBehaviour::<Req, Resp>::new(key, relay_client, &config)
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(config.idle_connection_timeout))
        .build();

    // ===== Data Channel（libp2p-stream）=====
    // new_control 是 &self，无需 mut。一份 control 交给 NetClient 打开出站通道；
    // 另一份临时 control 注册 accept 协议，accept 返回的 IncomingStreams 交给
    // event loop poll，其生命周期独立于临时 control。
    let control = swarm.behaviour().stream.new_control();
    let dc_registry = ChannelRegistry::new(config.data_channel_limits);
    let mut inbound_protocol_streams = Vec::new();
    {
        let mut accept_control = control.clone();
        for proto in &config.data_channel_protocols {
            match accept_control.accept(proto.clone()) {
                Ok(incoming) => inbound_protocol_streams.push((proto.clone(), incoming)),
                Err(e) => warn!("data-channel accept {} 失败: {}", proto, e),
            }
        }
    }

    // 创建 channels
    let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_SIZE);
    let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_SIZE);
    let (inbound_dc_tx, inbound_dc_rx) = mpsc::channel(DC_INBOUND_CHANNEL_SIZE);

    // 创建共享的 PendingMap（EventLoop 存入，NetClient 取出）
    // TTL 与 req_resp_timeout 保持一致，避免 channel 被提前清理
    let pending_channels = PendingMap::new(config.req_resp_timeout);

    // 创建 event loop
    let mut event_loop = EventLoop::new(
        swarm,
        command_rx,
        event_tx,
        pending_channels.clone(),
        config.protocol_version.clone(),
        inbound_protocol_streams,
        dc_registry.clone(),
        inbound_dc_tx,
    );

    // 启动监听
    event_loop.start_listen(&config.listen_addrs)?;

    // 连接引导节点
    if !config.bootstrap_peers.is_empty() {
        event_loop.connect_bootstrap_peers(&config.bootstrap_peers);
    }

    // 启动 event loop
    tokio::spawn(event_loop.run());

    // 返回 client、event receiver 和 data-channel receiver
    let client = NetClient::new(
        command_tx,
        pending_channels,
        control,
        dc_registry,
        config.data_channel_open_timeout,
    );
    let event_receiver = EventReceiver::new(event_rx);
    let dc_receiver = DataChannelReceiver::new(inbound_dc_rx);

    Ok((client, event_receiver, dc_receiver))
}
