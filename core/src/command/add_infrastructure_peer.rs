use async_trait::async_trait;
use libp2p::{Multiaddr, PeerId};
use tracing::{info, warn};

use crate::config::InfrastructureRoles;
use crate::runtime::CborMessage;

use super::{CommandHandler, CoreSwarm, ResultHandle};

/// 运行时注册基础设施 peer。
pub struct AddInfrastructurePeerCommand {
    peer_id: PeerId,
    addrs: Vec<Multiaddr>,
    roles: InfrastructureRoles,
    pending_relay_addrs: Option<Vec<Multiaddr>>,
}

impl AddInfrastructurePeerCommand {
    pub fn new(peer_id: PeerId, addrs: Vec<Multiaddr>, roles: InfrastructureRoles) -> Self {
        Self {
            peer_id,
            addrs,
            roles,
            pending_relay_addrs: None,
        }
    }
}

#[async_trait]
impl<Req: CborMessage, Resp: CborMessage> CommandHandler<Req, Resp>
    for AddInfrastructurePeerCommand
{
    type Result = ();

    async fn run(&mut self, swarm: &mut CoreSwarm<Req, Resp>, handle: &ResultHandle<Self::Result>) {
        for addr in &self.addrs {
            swarm.add_peer_address(self.peer_id, addr.clone());
            if self.roles.kad_server {
                swarm
                    .behaviour_mut()
                    .kad
                    .add_address(&self.peer_id, addr.clone());
            }
        }

        if self.roles.relay_server && swarm.behaviour().relay_client.is_enabled() {
            self.pending_relay_addrs = Some(self.addrs.clone());
        }

        if swarm.is_connected(&self.peer_id) {
            handle.finish(Ok(()));
            return;
        }

        match swarm.dial(self.peer_id) {
            Ok(()) => {
                info!("Dialing infrastructure peer {}", self.peer_id);
            }
            Err(e) => warn!("Failed to dial infrastructure peer {}: {}", self.peer_id, e),
        }

        handle.finish(Ok(()));
    }

    fn take_relay_reservation_request(&mut self) -> Option<(PeerId, Vec<Multiaddr>)> {
        self.pending_relay_addrs
            .take()
            .map(|addrs| (self.peer_id, addrs))
    }
}
