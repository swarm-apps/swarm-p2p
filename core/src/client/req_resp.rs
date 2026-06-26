use libp2p::PeerId;

use super::future::CommandFuture;
use crate::Result;
use crate::command::{SendRequestCommand, SendResponseCommand};
use crate::error::{Error, NetworkFailureKind};
use crate::request::RequestOptions;
use crate::runtime::CborMessage;

use super::NetClient;

impl<Req, Resp> NetClient<Req, Resp>
where
    Req: CborMessage,
    Resp: CborMessage,
{
    /// 发送请求并等待响应（使用全局默认 timeout）。
    pub async fn send_request(&self, peer_id: PeerId, request: Req) -> Result<Resp>
    where
        Req: Unpin,
    {
        self.send_request_with_options(peer_id, request, RequestOptions::default())
            .await
    }

    /// 发送请求并等待响应，支持 per-call 选项（自定义 timeout / correlation）。
    ///
    /// `options.timeout` 覆盖全局 req_resp timeout；超时返回
    /// `Error::Network(NetworkFailureKind::Timeout)`，且 `CommandFuture` 被 drop
    /// 后 event loop 会清理对应 active command。
    pub async fn send_request_with_options(
        &self,
        peer_id: PeerId,
        request: Req,
        options: RequestOptions,
    ) -> Result<Resp>
    where
        Req: Unpin,
    {
        if let Some(correlation) = &options.correlation {
            tracing::debug!(%peer_id, correlation, "send_request_with_options");
        }
        let cmd = SendRequestCommand::new(peer_id, request);
        let fut = CommandFuture::new(cmd, self.command_tx.clone());
        match options.timeout {
            Some(timeout) => match tokio::time::timeout(timeout, fut).await {
                Ok(result) => result,
                Err(_elapsed) => Err(Error::Network(NetworkFailureKind::Timeout)),
            },
            None => fut.await,
        }
    }

    /// 回复一个 inbound request
    ///
    /// `pending_id` 来自 `NodeEvent::InboundRequest` 中的标识，
    /// 用于从 PendingMap 取出对应的 `ResponseChannel` 进行回复。
    pub async fn send_response(&self, pending_id: u64, response: Resp) -> Result<()>
    where
        Resp: Unpin,
    {
        let channel = self.pending_channels.take(&pending_id).ok_or_else(|| {
            crate::error::Error::RequestResponse(format!(
                "No pending channel for pending_id={} (expired or already responded)",
                pending_id
            ))
        })?;
        let cmd = SendResponseCommand::new(channel, response);
        CommandFuture::new(cmd, self.command_tx.clone()).await
    }
}
