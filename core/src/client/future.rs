use std::task::{Context, Poll};

use crate::Result;
use crate::command::{Command, CommandHandler, CommandTask, ResultHandle};
use crate::runtime::CborMessage;

/// 命令 Future，使任意 CommandHandler 可被 await
pub struct CommandFuture<T, Req, Resp>
where
    T: CommandHandler<Req, Resp> + Send + 'static,
    Req: CborMessage,
    Resp: CborMessage,
{
    handler: Option<T>,
    handle: ResultHandle<T::Result>,
    sender: tokio::sync::mpsc::Sender<Command<Req, Resp>>,
}

impl<T, Req, Resp> CommandFuture<T, Req, Resp>
where
    T: CommandHandler<Req, Resp> + Send + 'static,
    T::Result: Send + 'static,
    Req: CborMessage,
    Resp: CborMessage,
{
    pub fn new(handler: T, sender: tokio::sync::mpsc::Sender<Command<Req, Resp>>) -> Self {
        Self {
            handler: Some(handler),
            handle: ResultHandle::new(),
            sender,
        }
    }
}

impl<T, Req, Resp> std::future::Future for CommandFuture<T, Req, Resp>
where
    T: CommandHandler<Req, Resp> + Send + Unpin + 'static,
    T::Result: Send + 'static,
    Req: CborMessage,
    Resp: CborMessage,
{
    type Output = Result<T::Result>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // 首次 poll 时发送命令
        if let Some(handler) = this.handler.take() {
            let task = CommandTask::new(handler, this.handle.clone());
            if this.sender.try_send(Box::new(task)).is_err() {
                return Poll::Ready(Err(crate::error::Error::Behaviour(
                    "command channel closed".into(),
                )));
            }
        }

        // 注册 waker 并检查结果
        // 必须在首次 poll 时也注册 waker，否则同步完成的命令（如 stop_provide）
        // 会在 handle.finish() 时找不到 waker，导致 Future 永远不会被唤醒
        this.handle.poll(cx)
    }
}

impl<T, Req, Resp> Drop for CommandFuture<T, Req, Resp>
where
    T: CommandHandler<Req, Resp> + Send + 'static,
    Req: CborMessage,
    Resp: CborMessage,
{
    fn drop(&mut self) {
        // 调用方放弃等待（取消 / 超时）：标记 handle，event loop 下个 tick 清理对应
        // active command，避免它残留到全局 req_resp timeout 才被回收。
        self.handle.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::command::{CoreSwarm, ResultHandle};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestMessage;

    struct NeverCompletesCommand;

    #[async_trait]
    impl CommandHandler<TestMessage, TestMessage> for NeverCompletesCommand {
        type Result = ();

        async fn run(
            &mut self,
            _swarm: &mut CoreSwarm<TestMessage, TestMessage>,
            _handle: &ResultHandle<Self::Result>,
        ) {
        }
    }

    #[tokio::test]
    async fn first_poll_sends_command_and_drop_marks_it_cancelled() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut future = Box::pin(CommandFuture::<_, TestMessage, TestMessage>::new(
            NeverCompletesCommand,
            tx,
        ));

        assert!(matches!(futures::poll!(&mut future), Poll::Pending));
        let command = rx
            .recv()
            .await
            .expect("command should be sent on first poll");
        assert!(!command.is_cancelled());

        drop(future);

        assert!(
            command.is_cancelled(),
            "dropping CommandFuture should mark active command as cancelled"
        );
    }

    #[tokio::test]
    async fn closed_command_channel_returns_behaviour_error() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);

        let err = CommandFuture::<_, TestMessage, TestMessage>::new(NeverCompletesCommand, tx)
            .await
            .expect_err("closed command channel should fail");

        assert!(matches!(err, crate::error::Error::Behaviour(_)));
    }
}
