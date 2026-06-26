//! 集成测试：双节点 mDNS 发现 + 通用数据通道（libp2p-stream）。
//!
//! 验证：A `open_data_channel` → B 从 `DataChannelReceiver` 收到入站通道 →
//! 双向字节读写 → A 关闭写端后 B 读到 EOF。
//! 失败场景：未注册协议打开失败、request-response per-call timeout 生效。

mod common;

use std::time::Duration;

use common::*;
use futures::{AsyncReadExt, AsyncWriteExt};
use swarm_p2p_core::libp2p::{PeerId, StreamProtocol};
use swarm_p2p_core::{NodeConfig, NodeEvent, RequestOptions, start};
use tokio::time::timeout;

const DATA_PROTO: StreamProtocol = StreamProtocol::new("/test/data/1");

/// 测试配置：TCP + mDNS，注册一个 data-channel 协议。
fn dc_config() -> NodeConfig {
    NodeConfig::new("/test/1.0.0", "test/1.0.0")
        .with_listen_addrs(vec!["/ip4/0.0.0.0/tcp/0".parse().unwrap()])
        .with_relay_client(false)
        .with_dcutr(false)
        .with_autonat(false)
        .with_kad_server_mode(true)
        .with_data_channel_protocols(vec![DATA_PROTO])
}

/// A 打开出站通道 → B 接受 → 双向读写 → 正常关闭（EOF）。
#[tokio::test(flavor = "multi_thread")]
async fn dual_node_data_channel_roundtrip() {
    let keypair_a = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let keypair_b = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let peer_a_id = PeerId::from_public_key(&keypair_a.public());
    let peer_b_id = PeerId::from_public_key(&keypair_b.public());

    let (client_a, mut events_a, _dc_a) =
        start::<Ping, Pong>(keypair_a, dc_config()).expect("start A");
    let (_client_b, events_b, mut dc_rx_b) =
        start::<Ping, Pong>(keypair_b, dc_config()).expect("start B");

    // B 后台消费普通事件，避免 event channel 阻塞
    let b_events = tokio::spawn(event_printer(events_b, "B", None));

    // A 精确等待与 B 的连接建立。
    // 注意：cargo 并行跑测试时，mDNS 会同时发现其他测试的节点，
    // 因此必须等 peer_b_id（而非 wait_for_connection 返回的任意 peer），
    // 否则可能把数据通道开到别的测试节点上。
    timeout(TIMEOUT, async {
        loop {
            match events_a.recv().await {
                Some(NodeEvent::PeerConnected { peer_id }) if peer_id == peer_b_id => return,
                Some(_) => continue,
                None => panic!("A event stream closed"),
            }
        }
    })
    .await
    .expect("A should connect to B within timeout");

    // A 打开出站数据通道（精确指向 B）
    let mut ch_a = timeout(TIMEOUT, client_a.open_data_channel(peer_b_id, DATA_PROTO))
        .await
        .expect("open_data_channel timed out")
        .expect("open_data_channel failed");
    assert_eq!(ch_a.protocol(), &DATA_PROTO);

    // B 从 DataChannelReceiver 收到入站通道
    let inbound = timeout(TIMEOUT, dc_rx_b.recv())
        .await
        .expect("inbound recv timed out")
        .expect("B should receive an inbound data channel");
    let mut ch_b = inbound.channel;
    assert_eq!(ch_b.peer(), peer_a_id, "入站通道对端应为 A");
    assert_eq!(ch_b.protocol(), &DATA_PROTO);

    // A → B
    ch_a.stream_mut().write_all(b"ping-data").await.unwrap();
    ch_a.stream_mut().flush().await.unwrap();
    let mut buf = [0u8; 9];
    ch_b.stream_mut().read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping-data");

    // B → A
    ch_b.stream_mut().write_all(b"pong-data").await.unwrap();
    ch_b.stream_mut().flush().await.unwrap();
    let mut buf2 = [0u8; 9];
    ch_a.stream_mut().read_exact(&mut buf2).await.unwrap();
    assert_eq!(&buf2, b"pong-data");

    // A 关闭写端 → B 读到 EOF
    ch_a.stream_mut().close().await.unwrap();
    let mut tail = [0u8; 1];
    let n = ch_b.stream_mut().read(&mut tail).await.unwrap();
    assert_eq!(n, 0, "A 关闭后 B 应读到 EOF");

    b_events.abort();
}

/// 打开未注册协议应失败（对端不 accept → 协商失败）。
#[tokio::test(flavor = "multi_thread")]
async fn open_unsupported_protocol_fails() {
    let keypair_a = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let keypair_b = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();

    let (client_a, events_a, _dc_a) = start::<Ping, Pong>(keypair_a, dc_config()).expect("start A");
    let (_client_b, events_b, _dc_b) =
        start::<Ping, Pong>(keypair_b, dc_config()).expect("start B");
    let b_events = tokio::spawn(event_printer(events_b, "B", None));

    let (_disc, peer_b, _id) = wait_for_connection(events_a).await;
    let peer_b = peer_b.expect("A should connect to B");

    let bad_proto = StreamProtocol::new("/test/nonexistent/9");
    let res = timeout(TIMEOUT, client_a.open_data_channel(peer_b, bad_proto)).await;
    let res = res.expect("open should not hang");
    assert!(res.is_err(), "打开未注册协议应返回错误，实际: {res:?}");

    b_events.abort();
}

/// request-response 的 per-call timeout 应远早于全局 timeout 生效。
#[tokio::test(flavor = "multi_thread")]
async fn send_request_per_call_timeout_applies() {
    let keypair = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let (client, events, _dc) = start::<Ping, Pong>(keypair, dc_config()).expect("start");
    let _bg = tokio::spawn(event_printer(events, "T", None));

    // 随机未连接 peer（无地址），请求无法完成
    let random_peer = PeerId::from_public_key(
        &swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519().public(),
    );
    let opts = RequestOptions::new().with_timeout(Duration::from_millis(200));

    let started = std::time::Instant::now();
    let res = client
        .send_request_with_options(random_peer, Ping { msg: "x".into() }, opts)
        .await;
    let elapsed = started.elapsed();

    assert!(res.is_err(), "向未连接 peer 的请求应失败");
    assert!(
        elapsed < Duration::from_secs(5),
        "per-call timeout(200ms) 应远早于全局 req_resp timeout(120s) 生效，实际 {elapsed:?}"
    );
}
