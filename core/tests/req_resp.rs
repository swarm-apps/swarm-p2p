//! 集成测试：双节点显式 dial + Request-Response
//!
//! 在同一进程内启动两个 libp2p 节点（仅 TCP，关闭 mDNS），
//! 验证：监听 → 显式连接 → 请求-响应。

mod common;

use common::*;
use swarm_p2p_core::libp2p::PeerId;
use swarm_p2p_core::{Error, NetClient, NetworkFailureKind, NodeEvent, RequestOptions, start};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// 启动两个节点，并行监听事件，验证完整流程
#[tokio::test(flavor = "multi_thread")]
async fn dual_node_full_flow() {
    // ===== 启动两个节点 =====
    let keypair_a = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let keypair_b = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let peer_a_id = PeerId::from_public_key(&keypair_a.public());
    let peer_b_id = PeerId::from_public_key(&keypair_b.public());

    let (client_a, mut events_a, _dc_a) =
        start::<Ping, Pong>(keypair_a, explicit_dial_config()).expect("failed to start node A");
    let (client_b, mut events_b, _dc_b) =
        start::<Ping, Pong>(keypair_b, explicit_dial_config()).expect("failed to start node B");

    let addr_a = wait_for_listen_addr(&mut events_a).await;
    let addr_b = wait_for_listen_addr(&mut events_b).await;
    connect_by_explicit_dial(&client_a, peer_a_id, addr_a, &client_b, peer_b_id, addr_b).await;

    // 用 channel 从 B 的事件监听 task 传回 inbound request 信息
    let (inbound_tx, mut inbound_rx) = mpsc::channel::<(u64, Ping)>(1);

    // ===== B 事件监听（后台 task，打印所有事件，处理 inbound request） =====
    let b_task = tokio::spawn(node_b_listener(events_b, client_b, inbound_tx));

    // ===== Request-Response =====
    let response = timeout(
        TIMEOUT,
        client_a.send_request(
            peer_b_id,
            Ping {
                msg: "hello".into(),
            },
        ),
    )
    .await
    .expect("send_request timed out")
    .expect("send_request failed");

    assert_eq!(response.msg, "world");

    // 验证 B 确实收到了请求
    let (pending_id, request) = inbound_rx
        .recv()
        .await
        .expect("B should report inbound request");
    assert_eq!(request.msg, "hello");
    eprintln!("[B] handled inbound request pending_id={pending_id}");

    b_task.abort(); // 测试完成，停止 B 的事件监听
}

#[tokio::test(flavor = "multi_thread")]
async fn send_response_returns_error_when_pending_id_is_missing() {
    let keypair = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let (client, events, _dc) =
        start::<Ping, Pong>(keypair, explicit_dial_config()).expect("failed to start node");
    let bg = tokio::spawn(event_printer(events, "single", None));

    let err = client
        .send_response(999, Pong { msg: "lost".into() })
        .await
        .expect_err("missing pending id should fail");

    assert!(
        matches!(err, Error::RequestResponse(_)),
        "actual error: {err:?}"
    );
    bg.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn send_request_per_call_timeout_returns_typed_error() {
    let keypair_a = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let keypair_b = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let peer_a_id = PeerId::from_public_key(&keypair_a.public());
    let peer_b_id = PeerId::from_public_key(&keypair_b.public());

    let (client_a, mut events_a, _dc_a) =
        start::<Ping, Pong>(keypair_a, explicit_dial_config()).expect("failed to start node A");
    let (client_b, mut events_b, _dc_b) =
        start::<Ping, Pong>(keypair_b, explicit_dial_config()).expect("failed to start node B");

    let addr_a = wait_for_listen_addr(&mut events_a).await;
    let addr_b = wait_for_listen_addr(&mut events_b).await;
    connect_by_explicit_dial(&client_a, peer_a_id, addr_a, &client_b, peer_b_id, addr_b).await;

    let bg_a = tokio::spawn(event_printer(events_a, "timeout-A", None));
    let bg_b = tokio::spawn(event_printer(events_b, "timeout-B", None));

    let err = client_a
        .send_request_with_options(
            peer_b_id,
            Ping { msg: "x".into() },
            RequestOptions::new().with_timeout(std::time::Duration::from_millis(100)),
        )
        .await
        .expect_err("request without response should time out");

    assert!(
        matches!(err, Error::Network(NetworkFailureKind::Timeout)),
        "actual error: {err:?}"
    );
    bg_a.abort();
    bg_b.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn mdns_disabled_nodes_do_not_connect_without_explicit_dial() {
    let keypair_a = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let keypair_b = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let peer_a_id = PeerId::from_public_key(&keypair_a.public());
    let peer_b_id = PeerId::from_public_key(&keypair_b.public());

    let (client_a, mut events_a, _dc_a) =
        start::<Ping, Pong>(keypair_a, explicit_dial_config()).expect("failed to start node A");
    let (client_b, mut events_b, _dc_b) =
        start::<Ping, Pong>(keypair_b, explicit_dial_config()).expect("failed to start node B");

    let _ = wait_for_listen_addr(&mut events_a).await;
    let _ = wait_for_listen_addr(&mut events_b).await;
    let a_events = tokio::spawn(event_printer(events_a, "A-no-mdns", None));
    let b_events = tokio::spawn(event_printer(events_b, "B-no-mdns", None));

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    assert!(
        !client_a
            .is_connected(peer_b_id)
            .await
            .expect("is_connected should complete")
    );
    assert!(
        !client_b
            .is_connected(peer_a_id)
            .await
            .expect("is_connected should complete")
    );

    a_events.abort();
    b_events.abort();
}

/// B 侧：打印所有事件，处理 inbound request
async fn node_b_listener(
    mut events: swarm_p2p_core::EventReceiver<Ping>,
    client: NetClient<Ping, Pong>,
    inbound_tx: mpsc::Sender<(u64, Ping)>,
) {
    loop {
        let Some(event) = events.recv().await else {
            break;
        };
        eprintln!("[B] {:?}", event);

        if let NodeEvent::InboundRequest {
            pending_id,
            request,
            ..
        } = event
        {
            // 通知主测试线程
            let _ = inbound_tx.send((pending_id, request)).await;
            // 回复
            client
                .send_response(
                    pending_id,
                    Pong {
                        msg: "world".into(),
                    },
                )
                .await
                .expect("send_response should succeed");
        }
    }
}
