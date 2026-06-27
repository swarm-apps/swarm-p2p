//! 集成测试：双节点显式 dial + 通用数据通道（libp2p-stream）。
//!
//! 验证：A `open_data_channel` → B 从 `DataChannelReceiver` 收到入站通道 →
//! 双向字节读写 → A 关闭写端后 B 读到 EOF。
//! 失败场景：未注册协议、出站资源限制、失败开流后的配额释放。

mod common;

use common::*;
use futures::{AsyncReadExt, AsyncWriteExt};
use swarm_p2p_core::libp2p::{PeerId, StreamProtocol};
use swarm_p2p_core::{
    DataChannelCloseReason, DataChannelLimits, Error, NetClient, NodeConfig, start,
};
use tokio::time::timeout;

const DATA_PROTO: StreamProtocol = StreamProtocol::new("/test/data/1");

/// 测试配置：TCP + 显式 dial，注册一个 data-channel 协议。
fn dc_config() -> NodeConfig {
    explicit_dial_config().with_data_channel_protocols(vec![DATA_PROTO])
}

async fn start_connected_pair(
    config_a: NodeConfig,
    config_b: NodeConfig,
) -> (
    NetClient<Ping, Pong>,
    swarm_p2p_core::DataChannelReceiver,
    NetClient<Ping, Pong>,
    swarm_p2p_core::DataChannelReceiver,
    PeerId,
    PeerId,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let keypair_a = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let keypair_b = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let peer_a_id = PeerId::from_public_key(&keypair_a.public());
    let peer_b_id = PeerId::from_public_key(&keypair_b.public());

    let (client_a, mut events_a, dc_a) = start::<Ping, Pong>(keypair_a, config_a).expect("start A");
    let (client_b, mut events_b, dc_b) = start::<Ping, Pong>(keypair_b, config_b).expect("start B");

    let addr_a = wait_for_listen_addr(&mut events_a).await;
    let addr_b = wait_for_listen_addr(&mut events_b).await;
    connect_by_explicit_dial(&client_a, peer_a_id, addr_a, &client_b, peer_b_id, addr_b).await;

    let events_a_task = tokio::spawn(event_printer(events_a, "A", None));
    let events_b_task = tokio::spawn(event_printer(events_b, "B", None));

    (
        client_a,
        dc_a,
        client_b,
        dc_b,
        peer_a_id,
        peer_b_id,
        events_a_task,
        events_b_task,
    )
}

/// A 打开出站通道 → B 接受 → 双向读写 → 正常关闭（EOF）。
#[tokio::test(flavor = "multi_thread")]
async fn dual_node_data_channel_roundtrip() {
    let (client_a, _dc_a, _client_b, mut dc_rx_b, peer_a_id, peer_b_id, a_events, b_events) =
        start_connected_pair(dc_config(), dc_config()).await;

    let mut ch_a = timeout(TIMEOUT, client_a.open_data_channel(peer_b_id, DATA_PROTO))
        .await
        .expect("open_data_channel timed out")
        .expect("open_data_channel failed");
    assert_eq!(ch_a.protocol(), &DATA_PROTO);

    let inbound = timeout(TIMEOUT, dc_rx_b.recv())
        .await
        .expect("inbound recv timed out")
        .expect("B should receive an inbound data channel");
    let mut ch_b = inbound.channel;
    assert_eq!(ch_b.peer(), peer_a_id, "入站通道对端应为 A");
    assert_eq!(ch_b.protocol(), &DATA_PROTO);

    ch_a.stream_mut().write_all(b"ping-data").await.unwrap();
    ch_a.stream_mut().flush().await.unwrap();
    let mut buf = [0u8; 9];
    ch_b.stream_mut().read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping-data");

    ch_b.stream_mut().write_all(b"pong-data").await.unwrap();
    ch_b.stream_mut().flush().await.unwrap();
    let mut buf2 = [0u8; 9];
    ch_a.stream_mut().read_exact(&mut buf2).await.unwrap();
    assert_eq!(&buf2, b"pong-data");

    ch_a.stream_mut().close().await.unwrap();
    let mut tail = [0u8; 1];
    let n = ch_b.stream_mut().read(&mut tail).await.unwrap();
    assert_eq!(n, 0, "A 关闭后 B 应读到 EOF");

    a_events.abort();
    b_events.abort();
}

/// 打开未注册协议应失败（对端不 accept → 协商失败）。
#[tokio::test(flavor = "multi_thread")]
async fn open_unsupported_protocol_fails_with_typed_error() {
    let (client_a, _dc_a, _client_b, _dc_b, _peer_a, peer_b, a_events, b_events) =
        start_connected_pair(dc_config(), dc_config()).await;

    let bad_proto = StreamProtocol::new("/test/nonexistent/9");
    let err = timeout(TIMEOUT, client_a.open_data_channel(peer_b, bad_proto))
        .await
        .expect("open should not hang")
        .expect_err("打开未注册协议应返回错误");

    assert!(
        matches!(
            err,
            Error::DataChannel(DataChannelCloseReason::UnsupportedProtocol)
        ),
        "actual error: {err:?}"
    );

    a_events.abort();
    b_events.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn outbound_limit_rejects_second_active_channel_and_releases_after_drop() {
    let limited_a = dc_config().with_data_channel_limits(DataChannelLimits {
        max_inbound_per_peer: 4,
        max_outbound_per_peer: 1,
        max_per_protocol: 64,
    });
    let (client_a, _dc_a, _client_b, mut dc_rx_b, _peer_a, peer_b, a_events, b_events) =
        start_connected_pair(limited_a, dc_config()).await;

    let first = timeout(TIMEOUT, client_a.open_data_channel(peer_b, DATA_PROTO))
        .await
        .expect("first open should not time out")
        .expect("first open should succeed");
    let first_inbound = timeout(TIMEOUT, dc_rx_b.recv())
        .await
        .expect("B should receive first inbound")
        .expect("B inbound stream should be open");

    let second = client_a.open_data_channel(peer_b, DATA_PROTO).await;
    assert!(
        matches!(
            second,
            Err(Error::DataChannel(
                DataChannelCloseReason::ResourceLimitExceeded
            ))
        ),
        "second active channel should hit outbound limit: {second:?}"
    );

    drop(first);
    drop(first_inbound);

    timeout(TIMEOUT, client_a.open_data_channel(peer_b, DATA_PROTO))
        .await
        .expect("third open should not time out")
        .expect("third open should succeed after first is dropped");

    a_events.abort();
    b_events.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn outbound_quota_is_released_after_open_failure() {
    let limited_a = dc_config().with_data_channel_limits(DataChannelLimits {
        max_inbound_per_peer: 4,
        max_outbound_per_peer: 1,
        max_per_protocol: 64,
    });
    let (client_a, _dc_a, _client_b, mut dc_rx_b, _peer_a, peer_b, a_events, b_events) =
        start_connected_pair(limited_a, dc_config()).await;

    let bad_proto = StreamProtocol::new("/test/nonexistent/9");
    let failed = client_a.open_data_channel(peer_b, bad_proto).await;
    assert!(failed.is_err(), "bad protocol should fail");

    timeout(TIMEOUT, client_a.open_data_channel(peer_b, DATA_PROTO))
        .await
        .expect("good open should not time out")
        .expect("good open should succeed after failed open releases quota");
    timeout(TIMEOUT, dc_rx_b.recv())
        .await
        .expect("B should receive good inbound")
        .expect("B inbound stream should be open");

    a_events.abort();
    b_events.abort();
}
