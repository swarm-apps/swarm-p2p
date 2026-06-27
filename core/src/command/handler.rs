use std::marker::PhantomData;

use async_trait::async_trait;
use parking_lot::Mutex;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use libp2p::Swarm;
use libp2p::swarm::SwarmEvent;

use crate::runtime::{CborMessage, CoreBehaviour, CoreBehaviourEvent};

/// Swarm 类型别名
pub type CoreSwarm<Req, Resp> = Swarm<CoreBehaviour<Req, Resp>>;

/// on_event 返回值：(keep_active, remaining_event)
///
/// - `keep_active`: 命令是否继续留在 active_commands 中等待后续事件
/// - `remaining_event`:
///   - `None` — 事件已被该命令消费，不再传递
///   - `Some(event)` — 事件未消费，传递给下一个命令或 convert_to_node_event
pub type OnEventResult<Req, Resp> = (bool, Option<SwarmEvent<CoreBehaviourEvent<Req, Resp>>>);

/// 命令结果句柄，用于命令完成时返回结果
#[derive(Debug)]
pub struct ResultHandle<T>(Arc<Mutex<ResultState<T>>>);

#[derive(Debug)]
struct ResultState<T> {
    result: Option<crate::Result<T>>,
    waker: Option<Waker>,
    /// 调用方（CommandFuture）是否已 drop / 取消。
    cancelled: bool,
}

impl<T> Default for ResultState<T> {
    fn default() -> Self {
        Self {
            result: None,
            waker: None,
            cancelled: false,
        }
    }
}

impl<T> Clone for ResultHandle<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Default for ResultHandle<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ResultHandle<T> {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(ResultState::default())))
    }

    pub fn poll(&self, cx: &Context<'_>) -> Poll<crate::Result<T>> {
        let mut state = self.0.lock();
        if let Some(result) = state.result.take() {
            Poll::Ready(result)
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    /// 完成命令并返回结果
    pub fn finish(&self, result: crate::Result<T>) {
        let mut state = self.0.lock();
        state.result = Some(result);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    /// 标记调用方已取消（CommandFuture drop 时调用）。
    pub fn cancel(&self) {
        self.0.lock().cancelled = true;
    }

    /// 调用方是否已取消，event loop 据此清理对应 active command。
    pub fn is_cancelled(&self) -> bool {
        self.0.lock().cancelled
    }
}

/// 命令处理器 trait
///
/// `on_event` 接收 owned event，命令可选择消费或传递：
/// - 消费：返回 `(keep, None)`，事件不再传递给后续命令和前端
/// - 传递：返回 `(keep, Some(event))`，事件继续流转
#[async_trait]
pub trait CommandHandler<Req, Resp>: Send + 'static
where
    Req: CborMessage,
    Resp: CborMessage,
{
    type Result: Send + 'static;

    /// 执行命令
    async fn run(&mut self, swarm: &mut CoreSwarm<Req, Resp>, handle: &ResultHandle<Self::Result>);

    /// 处理 swarm 事件
    ///
    /// 返回 `(keep_active, remaining_event)`：
    /// - `keep_active`: true 继续等待后续事件，false 命令完成
    /// - `remaining_event`: None 表示已消费，Some 表示传递给下一个处理者
    async fn on_event(
        &mut self,
        event: SwarmEvent<CoreBehaviourEvent<Req, Resp>>,
        _swarm: &mut CoreSwarm<Req, Resp>,
        _handle: &ResultHandle<Self::Result>,
    ) -> OnEventResult<Req, Resp> {
        (false, Some(event))
    }

    /// 命令执行后希望 event loop 在 peer 连接建立时申请 relay reservation。
    fn take_relay_reservation_request(
        &mut self,
    ) -> Option<(libp2p::PeerId, Vec<libp2p::Multiaddr>)> {
        None
    }
}

/// 命令 trait object 包装
pub type Command<Req, Resp> = Box<dyn CommandTrait<Req, Resp> + Send>;

/// 用于 trait object 的命令接口
#[async_trait]
pub trait CommandTrait<Req, Resp>: Send
where
    Req: CborMessage,
    Resp: CborMessage,
{
    async fn run_boxed(&mut self, swarm: &mut CoreSwarm<Req, Resp>);
    async fn on_event_boxed(
        &mut self,
        event: SwarmEvent<CoreBehaviourEvent<Req, Resp>>,
        swarm: &mut CoreSwarm<Req, Resp>,
    ) -> OnEventResult<Req, Resp>;
    /// 命令执行后取出延迟 relay reservation 请求。
    fn take_relay_reservation_request(
        &mut self,
    ) -> Option<(libp2p::PeerId, Vec<libp2p::Multiaddr>)>;
    /// 调用方 future 是否已 drop / 取消。
    fn is_cancelled(&self) -> bool;
}

/// 命令任务，包装 CommandHandler + ResultHandle
pub struct CommandTask<T, Req, Resp>
where
    T: CommandHandler<Req, Resp>,
    Req: CborMessage,
    Resp: CborMessage,
{
    handler: T,
    handle: ResultHandle<T::Result>,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<T, Req, Resp> CommandTask<T, Req, Resp>
where
    T: CommandHandler<Req, Resp>,
    Req: CborMessage,
    Resp: CborMessage,
{
    pub fn new(handler: T, handle: ResultHandle<T::Result>) -> Self {
        Self {
            handler,
            handle,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, Req, Resp> CommandTrait<Req, Resp> for CommandTask<T, Req, Resp>
where
    T: CommandHandler<Req, Resp> + Send + 'static,
    T::Result: Send + 'static,
    Req: CborMessage,
    Resp: CborMessage,
{
    async fn run_boxed(&mut self, swarm: &mut CoreSwarm<Req, Resp>) {
        self.handler.run(swarm, &self.handle).await;
    }

    async fn on_event_boxed(
        &mut self,
        event: SwarmEvent<CoreBehaviourEvent<Req, Resp>>,
        swarm: &mut CoreSwarm<Req, Resp>,
    ) -> OnEventResult<Req, Resp> {
        self.handler.on_event(event, swarm, &self.handle).await
    }

    fn take_relay_reservation_request(
        &mut self,
    ) -> Option<(libp2p::PeerId, Vec<libp2p::Multiaddr>)> {
        self.handler.take_relay_reservation_request()
    }

    fn is_cancelled(&self) -> bool {
        self.handle.is_cancelled()
    }
}
