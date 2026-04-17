//! 集成测试：Kademlia DHT 操作
//!
//! 三节点架构：引导节点(S) + A + B，关闭 mDNS。
//! A 和 B 通过引导节点加入 DHT 网络，验证：
//! bootstrap、put_record/get_record、start_provide/get_providers、
//! get_closest_peers、stop_provide、remove_record。

mod common;

use std::time::Duration;

use common::*;
use libp2p::PeerId;
use libp2p::kad::{Record, RecordKey};
use swarm_p2p_core::{NodeConfig, NodeEvent, start};
use tokio::sync::oneshot;
use tokio::time::timeout;

/// 关闭 mDNS 的 Kad 测试配置
fn kad_config() -> NodeConfig {
    NodeConfig::new("/test/1.0.0", "test/1.0.0")
        .with_listen_addrs(vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()])
        .with_mdns(false)
        .with_relay_client(false)
        .with_dcutr(false)
        .with_autonat(false)
        .with_kad_server_mode(true)
}

/// 带引导节点的 Kad 测试配置
fn kad_config_with_bootstrap(boot_peer: PeerId, boot_addr: libp2p::Multiaddr) -> NodeConfig {
    let mut cfg = kad_config();
    cfg.bootstrap_peers = vec![(boot_peer, boot_addr)];
    cfg.kad_query_timeout = Duration::from_secs(5);
    cfg
}

const KAD_TIMEOUT: Duration = Duration::from_secs(15);

/// 从事件流中提取第一个 Listening 地址
async fn wait_for_listen_addr(
    events: &mut swarm_p2p_core::EventReceiver<Ping>,
) -> libp2p::Multiaddr {
    loop {
        if let Some(NodeEvent::Listening { addr }) = events.recv().await {
            return addr;
        }
    }
}

/// 等待指定节点的 IdentifyReceived
async fn wait_for_identify(events: &mut swarm_p2p_core::EventReceiver<Ping>, label: &str) {
    let result = timeout(KAD_TIMEOUT, async {
        loop {
            if let Some(event) = events.recv().await {
                eprintln!("[{}] {:?}", label, event);
                if matches!(&event, NodeEvent::IdentifyReceived { .. }) {
                    return;
                }
            }
        }
    })
    .await;
    assert!(result.is_ok(), "[{}] IdentifyReceived timed out", label);
}

#[tokio::test(flavor = "multi_thread")]
async fn three_node_kad_flow() {
    // ===== 1. 启动引导节点 S =====
    let keypair_s = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let peer_s_id = PeerId::from_public_key(&keypair_s.public());

    let (_client_s, mut events_s) =
        start::<Ping, Pong>(keypair_s, kad_config()).expect("failed to start boot node S");

    // 获取 S 的监听地址
    let boot_addr = timeout(KAD_TIMEOUT, wait_for_listen_addr(&mut events_s))
        .await
        .expect("boot node listen timed out");
    eprintln!("[S] listening at {}", boot_addr);

    // S 的事件后台消费（防止 channel 满阻塞）
    let s_task = tokio::spawn(event_printer(events_s, "S", None));

    // ===== 2. 启动 A 和 B（指向引导节点） =====
    let keypair_a = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let keypair_b = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let peer_a_id = PeerId::from_public_key(&keypair_a.public());

    let (client_a, mut events_a) = start::<Ping, Pong>(
        keypair_a,
        kad_config_with_bootstrap(peer_s_id, boot_addr.clone()),
    )
    .expect("failed to start node A");

    let (client_b, mut events_b) =
        start::<Ping, Pong>(keypair_b, kad_config_with_bootstrap(peer_s_id, boot_addr))
            .expect("failed to start node B");

    // ===== 3. 等待 A 和 B 与引导节点完成 Identify =====
    // wait_for_identify 内部已 assert
    tokio::join!(
        wait_for_identify(&mut events_a, "A"),
        wait_for_identify(&mut events_b, "B"),
    );
    eprintln!("===== A and B connected to boot node, bootstrapping Kad =====");

    // ===== 4. Bootstrap Kad 路由表 =====
    // A 通知 B 的后台事件监听
    let (b_identify_tx, b_identify_rx) = oneshot::channel::<()>();
    let b_task = tokio::spawn(event_printer(events_b, "B", Some(b_identify_tx)));
    // A 也后台监听
    let (a_identify_tx, a_identify_rx) = oneshot::channel::<()>();
    let a_task = tokio::spawn(event_printer(events_a, "A", Some(a_identify_tx)));

    let (bootstrap_a, bootstrap_b) = tokio::join!(
        timeout(KAD_TIMEOUT, client_a.bootstrap()),
        timeout(KAD_TIMEOUT, client_b.bootstrap()),
    );
    bootstrap_a
        .expect("bootstrap A timed out")
        .expect("bootstrap A failed");
    bootstrap_b
        .expect("bootstrap B timed out")
        .expect("bootstrap B failed");
    eprintln!("[Kad] Both nodes bootstrapped");

    // 等待 A 和 B 通过 Kad 发现彼此后交换 Identify
    // bootstrap 会让 A 发现 B、B 发现 A，然后互相连接
    let _ = timeout(KAD_TIMEOUT, a_identify_rx).await;
    let _ = timeout(KAD_TIMEOUT, b_identify_rx).await;
    eprintln!("[Kad] A and B discovered each other via DHT");

    // ===== 5. put_record (A) → get_record (B) =====
    let key = RecordKey::new(&b"/test/greeting");
    let record = Record::new(key.clone(), b"hello-kad".to_vec());

    let put_stats = timeout(KAD_TIMEOUT, client_a.put_record(record))
        .await
        .expect("put_record timed out")
        .expect("put_record failed");
    eprintln!("[Kad] put_record stats: {:?}", put_stats);

    let get_result = timeout(KAD_TIMEOUT, client_b.get_record(key.clone()))
        .await
        .expect("get_record timed out")
        .expect("get_record failed");
    assert_eq!(get_result.record.value, b"hello-kad".to_vec());
    eprintln!(
        "[Kad] get_record OK, value={:?}, stats={:?}",
        String::from_utf8_lossy(&get_result.record.value),
        get_result.stats
    );

    // ===== 6. start_provide (A) → get_providers (B) =====
    let provide_key = RecordKey::new(&b"/test/file/abc123");

    let provide_stats = timeout(KAD_TIMEOUT, client_a.start_provide(provide_key.clone()))
        .await
        .expect("start_provide timed out")
        .expect("start_provide failed");
    eprintln!("[Kad] start_provide stats: {:?}", provide_stats);

    let providers_result = timeout(KAD_TIMEOUT, client_b.get_providers(provide_key.clone()))
        .await
        .expect("get_providers timed out")
        .expect("get_providers failed");
    assert!(
        providers_result.providers.contains(&peer_a_id),
        "A should be a provider, got: {:?}",
        providers_result.providers
    );
    eprintln!(
        "[Kad] get_providers OK, providers={:?}, stats={:?}",
        providers_result.providers, providers_result.stats
    );

    // ===== 7. get_closest_peers =====
    let closest_key = RecordKey::new(&b"/test/closest");
    let closest_result = timeout(KAD_TIMEOUT, client_a.get_closest_peers(closest_key))
        .await
        .expect("get_closest_peers timed out")
        .expect("get_closest_peers failed");
    eprintln!(
        "[Kad] get_closest_peers OK, peers={:?}, stats={:?}",
        closest_result.peers, closest_result.stats
    );

    // ===== 8. stop_provide + remove_record =====
    client_a
        .stop_provide(provide_key)
        .await
        .expect("stop_provide failed");
    eprintln!("[Kad] stop_provide OK");

    client_a
        .remove_record(key)
        .await
        .expect("remove_record failed");
    eprintln!("[Kad] remove_record OK");

    a_task.abort();
    b_task.abort();
    s_task.abort();
}
