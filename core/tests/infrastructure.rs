//! 集成测试：LAN Helper / infrastructure peer 动态注册。

mod common;

use common::*;
use swarm_p2p_core::libp2p::PeerId;
use swarm_p2p_core::{
    InfrastructureRoles, LanHelperConfig, NetClient, NodeConfig, NodeEvent, start,
};
use tokio::time::{Duration, timeout};

fn helper_config() -> NodeConfig {
    explicit_dial_config().with_lan_helper(LanHelperConfig::default())
}

fn client_config(relay_client: bool) -> NodeConfig {
    NodeConfig::new("/test/1.0.0", "test/1.0.0")
        .with_listen_addrs(vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()])
        .with_mdns(false)
        .with_relay_client(relay_client)
        .with_dcutr(false)
        .with_autonat(false)
        .with_kad_server_mode(true)
}

async fn start_helper_and_client(
    relay_client: bool,
) -> (
    NetClient<Ping, Pong>,
    NetClient<Ping, Pong>,
    swarm_p2p_core::EventReceiver<Ping>,
    PeerId,
    swarm_p2p_core::EventReceiver<Ping>,
    PeerId,
    swarm_p2p_core::libp2p::Multiaddr,
) {
    let helper_key = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let client_key = swarm_p2p_core::libp2p::identity::Keypair::generate_ed25519();
    let helper_id = PeerId::from_public_key(&helper_key.public());
    let client_id = PeerId::from_public_key(&client_key.public());

    let (helper_client, mut helper_events, _helper_dc) =
        start::<Ping, Pong>(helper_key, helper_config()).expect("start helper");
    let (client, mut client_events, _client_dc) =
        start::<Ping, Pong>(client_key, client_config(relay_client)).expect("start client");

    let helper_addr = wait_for_listen_addr(&mut helper_events).await;
    let _client_addr = wait_for_listen_addr(&mut client_events).await;

    (
        helper_client,
        client,
        client_events,
        client_id,
        helper_events,
        helper_id,
        helper_addr,
    )
}

async fn retry_register_infrastructure_until_connected(
    client: &NetClient<Ping, Pong>,
    helper_id: PeerId,
    helper_addr: swarm_p2p_core::libp2p::Multiaddr,
    roles: InfrastructureRoles,
) {
    timeout(TIMEOUT, async {
        loop {
            if client
                .is_connected(helper_id)
                .await
                .expect("is_connected command should complete")
            {
                return;
            }
            if let Err(err) = client
                .add_infrastructure_peer(helper_id, vec![helper_addr.clone()], roles)
                .await
            {
                eprintln!("[test] add_infrastructure_peer failed: {err}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("infrastructure peer should connect within timeout");
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamic_infrastructure_peer_requests_relay_reservation() {
    let (
        _helper_client,
        client,
        _client_events,
        _client_id,
        mut helper_events,
        helper_id,
        helper_addr,
    ) = start_helper_and_client(true).await;

    retry_register_infrastructure_until_connected(
        &client,
        helper_id,
        helper_addr,
        InfrastructureRoles::kad_and_relay(),
    )
    .await;

    timeout(TIMEOUT, async {
        loop {
            match helper_events.recv().await {
                Some(NodeEvent::RelayServerReservationAccepted { src_peer_id, .. }) => {
                    assert_ne!(src_peer_id, helper_id);
                    return;
                }
                Some(_) => {}
                None => panic!("helper event stream closed"),
            }
        }
    })
    .await
    .expect("helper should accept dynamic relay reservation");
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamic_infrastructure_peer_skips_reservation_when_relay_client_disabled() {
    let (
        _helper_client,
        client,
        mut client_events,
        _client_id,
        _helper_events,
        helper_id,
        helper_addr,
    ) = start_helper_and_client(false).await;

    retry_register_infrastructure_until_connected(
        &client,
        helper_id,
        helper_addr,
        InfrastructureRoles::kad_and_relay(),
    )
    .await;

    let maybe_reservation = timeout(Duration::from_millis(300), async {
        loop {
            match client_events.recv().await {
                Some(NodeEvent::RelayReservationAccepted { relay_peer_id, .. })
                    if relay_peer_id == helper_id =>
                {
                    return Some(());
                }
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;

    assert!(
        maybe_reservation.is_err(),
        "relay client disabled should not request reservation"
    );
}
