pub mod behaviour;
pub mod util;

use anyhow::Result;
use futures::StreamExt;
use libp2p::{Multiaddr, SwarmBuilder, identity::Keypair, noise, swarm::SwarmEvent, tcp, yamux};
use std::time::Duration;
use tracing::{debug, info};

use behaviour::BootstrapBehaviourEvent;

/// 启动引导+中继节点
///
/// 构建 Swarm 并运行事件循环，直到收到关闭信号。
pub async fn run(
    keypair: Keypair,
    tcp_addr: Multiaddr,
    quic_addr: Multiaddr,
    idle_timeout: Duration,
    external_addrs: Vec<Multiaddr>,
) -> Result<()> {
    // 引导节点不调用 .with_relay_client()
    // 闭包签名为 |key| 而非 |key, relay_client|
    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_dns()?
        .with_behaviour(behaviour::BootstrapBehaviour::new)?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(idle_timeout))
        .build();

    swarm.listen_on(tcp_addr)?;
    swarm.listen_on(quic_addr)?;

    // 注册公网地址，relay reservation 响应会携带这些地址给 client
    for addr in &external_addrs {
        swarm.add_external_address(addr.clone());
        info!("Added external address: {}", addr);
    }

    info!("Bootstrap+Relay node started, waiting for connections...");

    let mut shutdown = std::pin::pin!(util::shutdown_signal());

    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                handle_event(event);
            }
            _ = &mut shutdown => {
                info!("Shutting down...");
                break;
            }
        }
    }

    Ok(())
}

fn handle_event(event: SwarmEvent<BootstrapBehaviourEvent>) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            info!("Listening on {}", address);
        }
        SwarmEvent::ConnectionEstablished {
            peer_id,
            num_established,
            ..
        } => {
            if num_established.get() == 1 {
                info!("Peer connected: {}", peer_id);
            }
        }
        SwarmEvent::ConnectionClosed {
            peer_id,
            num_established,
            ..
        } => {
            if num_established == 0 {
                info!("Peer disconnected: {}", peer_id);
            }
        }
        SwarmEvent::Behaviour(BootstrapBehaviourEvent::Identify(
            libp2p::identify::Event::Received { peer_id, info, .. },
        )) => {
            info!(
                "Identified peer {}: agent={}, protocol={}",
                peer_id, info.agent_version, info.protocol_version
            );
        }
        SwarmEvent::Behaviour(BootstrapBehaviourEvent::Kad(event)) => match &event {
            libp2p::kad::Event::RoutingUpdated { peer, .. } => {
                info!("Kad routing updated: {}", peer);
            }
            _ => {
                debug!("Kad: {:?}", event);
            }
        },
        SwarmEvent::Behaviour(BootstrapBehaviourEvent::Relay(event)) => {
            info!("Relay: {:?}", event);
        }
        SwarmEvent::Behaviour(BootstrapBehaviourEvent::Autonat(event)) => {
            info!(
                "AutoNAT: tested {} for client {}, result: {:?}",
                event.tested_addr, event.client, event.result
            );
        }
        SwarmEvent::IncomingConnection { .. }
        | SwarmEvent::OutgoingConnectionError { .. }
        | SwarmEvent::IncomingConnectionError { .. } => {
            debug!("Connection event: {:?}", event);
        }
        _ => {}
    }
}
