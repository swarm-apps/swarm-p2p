use async_trait::async_trait;
use libp2p::PeerId;
use libp2p::request_response::{Event, Message, OutboundFailure, OutboundRequestId};
use libp2p::swarm::SwarmEvent;
use tracing::{error, info};

use crate::error::{Error, NetworkFailureKind};
use crate::runtime::{CborMessage, CoreBehaviourEvent};

use super::super::{CommandHandler, CoreSwarm, OnEventResult, ResultHandle};

pub struct SendRequestCommand<Req>
where
    Req: CborMessage,
{
    peer_id: PeerId,
    request: Option<Req>,
    request_id: Option<OutboundRequestId>,
}

impl<Req: CborMessage> SendRequestCommand<Req> {
    pub fn new(peer_id: PeerId, request: Req) -> Self {
        Self {
            peer_id,
            request: Some(request),
            request_id: None,
        }
    }
}

#[async_trait]
impl<Req, Resp> CommandHandler<Req, Resp> for SendRequestCommand<Req>
where
    Req: CborMessage,
    Resp: CborMessage,
{
    type Result = Resp;

    async fn run(&mut self, swarm: &mut CoreSwarm<Req, Resp>, handle: &ResultHandle<Self::Result>) {
        let Some(request) = self.request.take() else {
            handle.finish(Err(Error::RequestResponse(
                "SendRequest: run called twice".into(),
            )));
            return;
        };
        let request_id = swarm
            .behaviour_mut()
            .req_resp
            .send_request(&self.peer_id, request);
        self.request_id = Some(request_id);
        info!(
            "Sent request to {}, request_id: {:?}",
            self.peer_id, request_id
        );
    }

    async fn on_event(
        &mut self,
        event: SwarmEvent<CoreBehaviourEvent<Req, Resp>>,
        handle: &ResultHandle<Self::Result>,
    ) -> OnEventResult<Req, Resp> {
        match event {
            // 收到响应
            SwarmEvent::Behaviour(CoreBehaviourEvent::ReqResp(Event::Message {
                peer,
                message:
                    Message::Response {
                        request_id,
                        response,
                    },
                ..
            })) if self.request_id.as_ref() == Some(&request_id) && peer == self.peer_id => {
                info!("Received response from {}", peer);
                handle.finish(Ok(response));
                (false, None) // 消费，完成
            }
            // 发送失败
            SwarmEvent::Behaviour(CoreBehaviourEvent::ReqResp(Event::OutboundFailure {
                peer,
                request_id,
                error,
                ..
            })) if self.request_id.as_ref() == Some(&request_id) && peer == self.peer_id => {
                error!("Request to {} failed: {:?}", peer, error);
                handle.finish(Err(map_outbound_failure(&error)));
                (false, None) // 消费，完成
            }
            other => (true, Some(other)), // 继续等待
        }
    }
}

/// 将 request-response 的 `OutboundFailure` 归一化为 typed network failure。
fn map_outbound_failure(err: &OutboundFailure) -> Error {
    let kind = match err {
        OutboundFailure::DialFailure => NetworkFailureKind::DialFailure,
        OutboundFailure::Timeout => NetworkFailureKind::Timeout,
        OutboundFailure::ConnectionClosed => NetworkFailureKind::ConnectionClosed,
        OutboundFailure::UnsupportedProtocols => NetworkFailureKind::UnsupportedProtocol,
        _ => NetworkFailureKind::Unknown,
    };
    Error::Network(kind)
}
