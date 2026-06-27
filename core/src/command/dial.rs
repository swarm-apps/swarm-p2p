use async_trait::async_trait;
use libp2p::{PeerId, swarm::SwarmEvent};

use crate::error::Error;
use crate::runtime::{CborMessage, CoreBehaviourEvent};

use super::{CommandHandler, CoreSwarm, OnEventResult, ResultHandle};

/// Dial 命令 - 连接到指定 peer
pub struct DialCommand {
    peer_id: PeerId,
}

impl DialCommand {
    pub fn new(peer_id: PeerId) -> Self {
        Self { peer_id }
    }
}

#[async_trait]
impl<Req: CborMessage, Resp: CborMessage> CommandHandler<Req, Resp> for DialCommand {
    type Result = ();

    async fn run(&mut self, swarm: &mut CoreSwarm<Req, Resp>, handle: &ResultHandle<Self::Result>) {
        if swarm.is_connected(&self.peer_id) {
            handle.finish(Ok(()));
            return;
        }
        if let Err(e) = swarm.dial(self.peer_id) {
            handle.finish(Err(Error::Dial(e.to_string())));
        }
    }

    async fn on_event(
        &mut self,
        event: SwarmEvent<CoreBehaviourEvent<Req, Resp>>,
        _swarm: &mut CoreSwarm<Req, Resp>,
        handle: &ResultHandle<Self::Result>,
    ) -> OnEventResult<Req, Resp> {
        match &event {
            SwarmEvent::ConnectionEstablished { peer_id, .. } if *peer_id == self.peer_id => {
                handle.finish(Ok(()));
                (false, Some(event)) // 不消费，前端需要 PeerConnected
            }
            SwarmEvent::OutgoingConnectionError {
                peer_id: Some(peer_id),
                error,
                ..
            } if *peer_id == self.peer_id => {
                handle.finish(Err(Error::Dial(error.to_string())));
                (false, Some(event)) // 不消费
            }
            _ => (true, Some(event)), // 继续等待
        }
    }
}
