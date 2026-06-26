//! 集成测试：双节点 mDNS 发现 + Request-Response
//!
//! 在同一进程内启动两个 libp2p 节点（仅 TCP + mDNS），
//! 并行监听双方事件，验证：发现 → 连接 → Identify → 请求-响应。

mod common;

use common::*;
use swarm_p2p_core::{NetClient, NodeEvent, start};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// 启动两个节点，并行监听事件，验证完整流程
#[tokio::test(flavor = "multi_thread")]
async fn dual_node_full_flow() {
    // ===== 启动两个节点 =====
    let keypair_a = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let keypair_b = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();

    let (client_a, events_a, _dc_a) =
        start::<Ping, Pong>(keypair_a, test_config()).expect("failed to start node A");
    let (client_b, events_b, _dc_b) =
        start::<Ping, Pong>(keypair_b, test_config()).expect("failed to start node B");

    // 用 channel 从 B 的事件监听 task 传回 inbound request 信息
    let (inbound_tx, mut inbound_rx) = mpsc::channel::<(u64, Ping)>(1);

    // ===== B 事件监听（后台 task，打印所有事件，处理 inbound request） =====
    let b_task = tokio::spawn(node_b_listener(events_b, client_b, inbound_tx));

    // ===== A 事件监听：等待发现 + 连接 + Identify =====
    let (a_discovered, peer_b_id, a_identified) = wait_for_connection(events_a).await;

    assert!(a_discovered, "Node A should discover peers via mDNS");
    assert!(a_identified, "Node A should receive IdentifyReceived");
    let peer_b_id = peer_b_id.expect("Node A should connect to Node B");

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
