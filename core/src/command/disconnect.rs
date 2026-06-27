use async_trait::async_trait;
use libp2p::PeerId;
use libp2p::swarm::SwarmEvent;

use crate::error::Error;
use crate::runtime::{CborMessage, CoreBehaviourEvent};

use super::{CommandHandler, CoreSwarm, OnEventResult, ResultHandle};

/// Disconnect 命令 - 断开与指定 peer 的所有连接
pub struct DisconnectCommand {
    peer_id: PeerId,
}

impl DisconnectCommand {
    pub fn new(peer_id: PeerId) -> Self {
        Self { peer_id }
    }
}

#[async_trait]
impl<Req: CborMessage, Resp: CborMessage> CommandHandler<Req, Resp> for DisconnectCommand {
    type Result = ();

    async fn run(&mut self, swarm: &mut CoreSwarm<Req, Resp>, handle: &ResultHandle<Self::Result>) {
        if let Err(()) = swarm.disconnect_peer_id(self.peer_id) {
            handle.finish(Err(Error::Dial(format!(
                "peer {} is not connected",
                self.peer_id
            ))));
        }
        // Ok → 等待 ConnectionClosed 事件确认
    }

    async fn on_event(
        &mut self,
        event: SwarmEvent<CoreBehaviourEvent<Req, Resp>>,
        _swarm: &mut CoreSwarm<Req, Resp>,
        handle: &ResultHandle<Self::Result>,
    ) -> OnEventResult<Req, Resp> {
        match &event {
            // 所有连接都已关闭（num_established == 0）
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } if *peer_id == self.peer_id && *num_established == 0 => {
                handle.finish(Ok(()));
                (false, Some(event)) // 不消费，前端需要 PeerDisconnected
            }
            _ => (true, Some(event)), // 继续等待
        }
    }
}
