use async_trait::async_trait;
use libp2p::request_response::ResponseChannel;

use crate::error::Error;
use crate::runtime::CborMessage;

use super::super::{CommandHandler, CoreSwarm, ResultHandle};

pub struct SendResponseCommand<Resp>
where
    Resp: CborMessage,
{
    channel: Option<ResponseChannel<Resp>>,
    response: Option<Resp>,
}

impl<Resp: CborMessage> SendResponseCommand<Resp> {
    pub fn new(channel: ResponseChannel<Resp>, response: Resp) -> Self {
        Self {
            channel: Some(channel),
            response: Some(response),
        }
    }
}

#[async_trait]
impl<Req, Resp> CommandHandler<Req, Resp> for SendResponseCommand<Resp>
where
    Req: CborMessage,
    Resp: CborMessage,
{
    type Result = ();

    async fn run(&mut self, swarm: &mut CoreSwarm<Req, Resp>, handle: &ResultHandle<Self::Result>) {
        let (Some(channel), Some(response)) = (self.channel.take(), self.response.take()) else {
            handle.finish(Err(Error::RequestResponse(
                "SendResponse: run called twice".into(),
            )));
            return;
        };
        match swarm
            .behaviour_mut()
            .req_resp
            .send_response(channel, response)
        {
            Ok(()) => handle.finish(Ok(())),
            Err(_) => handle.finish(Err(Error::RequestResponse(
                "Failed to send response: channel closed".into(),
            ))),
        }
    }
}
