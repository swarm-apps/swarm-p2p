use std::time::Duration;

use libp2p::PeerId;
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

#[allow(dead_code)]
pub const TIMEOUT: Duration = Duration::from_secs(15);

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
