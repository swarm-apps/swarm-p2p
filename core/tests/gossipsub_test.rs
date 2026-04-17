use std::time::Duration;

use swarm_p2p_core::{NodeConfig, NodeEvent, start};

/// 辅助：创建测试用的节点配置
fn test_config(enable_gossipsub: bool) -> NodeConfig {
    NodeConfig::new("/test-gossipsub/1.0.0", "test/1.0.0")
        .with_gossipsub(enable_gossipsub)
        .with_relay_client(false)
        .with_dcutr(false)
        .with_autonat(false)
}

/// 辅助：生成随机密钥对
fn random_keypair() -> swarm_p2p_core::libp2p::identity::Keypair {
    swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519()
}

#[tokio::test]
async fn subscribe_and_unsubscribe() {
    let keypair = random_keypair();
    let (client, _receiver) = start::<(), ()>(keypair, test_config(true)).unwrap();

    // 首次订阅返回 true
    let result = client.subscribe("test-topic").await;
    assert!(result.is_ok());
    assert!(result.unwrap());

    // 重复订阅返回 false
    let result = client.subscribe("test-topic").await;
    assert!(result.is_ok());
    assert!(!result.unwrap());

    // 退订返回 true
    let result = client.unsubscribe("test-topic").await;
    assert!(result.is_ok());
    assert!(result.unwrap());

    // 再次退订返回 false
    let result = client.unsubscribe("test-topic").await;
    assert!(result.is_ok());
    assert!(!result.unwrap());
}

#[tokio::test]
async fn gossipsub_disabled_returns_error() {
    let keypair = random_keypair();
    let (client, _receiver) = start::<(), ()>(keypair, test_config(false)).unwrap();

    let result = client.subscribe("test-topic").await;
    assert!(result.is_err());

    let result = client.publish("test-topic", vec![1, 2, 3]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn two_nodes_gossipsub_message() {
    let keypair_a = random_keypair();
    let keypair_b = random_keypair();

    let (client_a, _receiver_a) = start::<(), ()>(keypair_a, test_config(true)).unwrap();
    let (_client_b, mut receiver_b) = start::<(), ()>(keypair_b, test_config(true)).unwrap();

    let topic = "doc:test-123";

    // 两个节点都订阅同一个 topic
    client_a.subscribe(topic).await.unwrap();
    _client_b.subscribe(topic).await.unwrap();

    // 等待 mDNS 发现 + GossipSub mesh 建立
    // mDNS 发现需要一些时间，GossipSub heartbeat 也需要时间建立 mesh
    let mut peers_discovered = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), receiver_b.recv()).await {
            Ok(Some(NodeEvent::PeersDiscovered { .. })) => {
                peers_discovered = true;
            }
            Ok(Some(NodeEvent::GossipSubscribed { .. })) => {
                // mesh 已建立，可以发消息了
                break;
            }
            _ => {}
        }
    }

    if !peers_discovered {
        // mDNS 在 CI 环境可能不工作，跳过
        eprintln!("SKIP: mDNS discovery not available in this environment");
        return;
    }

    // 等待 GossipSub mesh 完全建立
    tokio::time::sleep(Duration::from_secs(2)).await;

    // A 发布消息
    let message_data = b"hello from A".to_vec();
    client_a.publish(topic, message_data.clone()).await.unwrap();

    // B 应该收到消息
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut received = false;

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), receiver_b.recv()).await {
            Ok(Some(NodeEvent::GossipMessage { data, topic: t, .. })) => {
                assert_eq!(data, message_data);
                assert_eq!(t, topic);
                received = true;
                break;
            }
            _ => {}
        }
    }

    assert!(
        received,
        "Node B should have received the GossipSub message from Node A"
    );
}
