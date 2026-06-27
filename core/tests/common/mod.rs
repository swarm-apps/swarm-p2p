use std::time::Duration;

use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use swarm_p2p_core::{NodeConfig, NodeEvent};
use tokio::sync::oneshot;
use tokio::time::timeout;

// ─── 测试用消息类型 ───

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Ping {
    pub msg: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Pong {
    pub msg: String,
}

// ─── 辅助函数 ───

/// 创建测试用配置（仅 TCP + mDNS，关闭其他功能加速测试）
#[allow(dead_code)]
pub fn test_config() -> NodeConfig {
    NodeConfig::new("/test/1.0.0", "test/1.0.0")
        .with_listen_addrs(vec!["/ip4/0.0.0.0/tcp/0".parse().unwrap()])
        .with_relay_client(false)
        .with_dcutr(false)
        .with_autonat(false)
        .with_kad_server_mode(true)
}

/// 创建不依赖 mDNS 的测试配置。集成测试优先用它，避免并行测试互相发现。
#[allow(dead_code)]
pub fn explicit_dial_config() -> NodeConfig {
    NodeConfig::new("/test/1.0.0", "test/1.0.0")
        .with_listen_addrs(vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()])
        .with_mdns(false)
        .with_relay_client(false)
        .with_dcutr(false)
        .with_autonat(false)
        .with_kad_server_mode(true)
}

#[allow(dead_code)]
pub const TIMEOUT: Duration = Duration::from_secs(15);

#[allow(dead_code)]
pub async fn wait_for_listen_addr(events: &mut swarm_p2p_core::EventReceiver<Ping>) -> Multiaddr {
    timeout(TIMEOUT, async {
        loop {
            match events.recv().await {
                Some(NodeEvent::Listening { addr }) => return addr,
                Some(_) => continue,
                None => panic!("event stream closed before Listening"),
            }
        }
    })
    .await
    .expect("node should start listening within timeout")
}

#[allow(dead_code)]
pub async fn connect_by_explicit_dial(
    client_a: &swarm_p2p_core::NetClient<Ping, Pong>,
    peer_a: PeerId,
    addr_a: Multiaddr,
    client_b: &swarm_p2p_core::NetClient<Ping, Pong>,
    peer_b: PeerId,
    addr_b: Multiaddr,
) {
    client_a
        .add_peer_addrs(peer_b, vec![addr_b.clone()])
        .await
        .expect("A should register B address");
    client_b
        .add_peer_addrs(peer_a, vec![addr_a.clone()])
        .await
        .expect("B should register A address");

    let connect_a = retry_dial_until_connected(client_a, peer_b, addr_b);
    let observe_b = wait_until_connected(client_b, peer_a);
    tokio::join!(connect_a, observe_b);
}

#[allow(dead_code)]
pub async fn retry_dial_until_connected(
    client: &swarm_p2p_core::NetClient<Ping, Pong>,
    peer: PeerId,
    addr: Multiaddr,
) {
    timeout(TIMEOUT, async {
        loop {
            if client
                .is_connected(peer)
                .await
                .expect("is_connected command should complete")
            {
                return;
            }
            let _ = client.add_peer_addrs(peer, vec![addr.clone()]).await;
            let _ = client.dial(peer).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("explicit dial should connect within timeout");
}

#[allow(dead_code)]
pub async fn wait_until_connected(client: &swarm_p2p_core::NetClient<Ping, Pong>, peer: PeerId) {
    timeout(TIMEOUT, async {
        loop {
            if client
                .is_connected(peer)
                .await
                .expect("is_connected command should complete")
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("peer should observe incoming connection within timeout");
}

/// A 侧：等待 mDNS 发现 + PeerConnected + IdentifyReceived
#[allow(dead_code)]
pub async fn wait_for_connection(
    mut events: swarm_p2p_core::EventReceiver<Ping>,
) -> (bool, Option<PeerId>, bool) {
    let mut discovered = false;
    let mut connected: Option<PeerId> = None;
    let mut identified = false;

    let result = timeout(TIMEOUT, async {
        loop {
            if let Some(event) = events.recv().await {
                eprintln!("[A] {:?}", event);
                match &event {
                    NodeEvent::PeersDiscovered { .. } => discovered = true,
                    NodeEvent::PeerConnected { peer_id } => connected = Some(*peer_id),
                    NodeEvent::IdentifyReceived {
                        protocol_version,
                        agent_version,
                        ..
                    } => {
                        assert_eq!(protocol_version, "/test/1.0.0");
                        assert_eq!(agent_version, "test/1.0.0");
                        identified = true;
                    }
                    _ => {}
                }
                if discovered && connected.is_some() && identified {
                    return;
                }
            }
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "Should complete discovery + connect + identify within timeout"
    );
    (discovered, connected, identified)
}

/// 通用事件打印器，可选在收到 IdentifyReceived 时通知
#[allow(dead_code)]
pub async fn event_printer(
    mut events: swarm_p2p_core::EventReceiver<Ping>,
    label: &str,
    identify_tx: Option<oneshot::Sender<()>>,
) {
    let mut identify_tx = identify_tx;
    loop {
        let Some(event) = events.recv().await else {
            break;
        };
        eprintln!("[{}] {:?}", label, event);

        if matches!(&event, NodeEvent::IdentifyReceived { .. })
            && let Some(tx) = identify_tx.take()
        {
            let _ = tx.send(());
        }
    }
}
