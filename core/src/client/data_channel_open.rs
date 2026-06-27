use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use libp2p::{PeerId, StreamProtocol};
use libp2p_stream::{Control, OpenStreamError};

use crate::{
    Result,
    data_channel::{
        ChannelGuard, ChannelRegistry, DataChannel, DataChannelCloseReason, DataChannelDirection,
        DataChannelId,
    },
    error::{Error, NetworkFailureKind},
};

type BoxOpenFuture = Pin<Box<dyn Future<Output = Result<DataChannel>> + Send + 'static>>;

/// 打开出站 data-channel 的 Future。
///
/// 它不经过 swarm command loop，而是直接使用 `libp2p-stream::Control`。
/// 首次 poll 时才占用配额；如果调用方在开流期间取消 / 超时并丢弃 Future，
/// 内部持有的 `ChannelGuard` 会随 Future 一起 drop，从而释放配额。
#[must_use = "Future does nothing unless polled or awaited"]
pub(crate) struct DataChannelOpenFuture {
    state: OpenState,
}

enum OpenState {
    Init(OpenInit),
    Opening(BoxOpenFuture),
    Finished,
}

struct OpenInit {
    peer_id: PeerId,
    protocol: StreamProtocol,
    control: Control,
    registry: ChannelRegistry,
    open_timeout: Duration,
}

impl DataChannelOpenFuture {
    pub(crate) fn new(
        peer_id: PeerId,
        protocol: StreamProtocol,
        control: Control,
        registry: ChannelRegistry,
        open_timeout: Duration,
    ) -> Self {
        Self {
            state: OpenState::Init(OpenInit {
                peer_id,
                protocol,
                control,
                registry,
                open_timeout,
            }),
        }
    }
}

impl Future for DataChannelOpenFuture {
    type Output = Result<DataChannel>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match std::mem::replace(&mut self.state, OpenState::Finished) {
                OpenState::Init(init) => match init.into_opening_future() {
                    Ok(future) => {
                        self.state = OpenState::Opening(future);
                    }
                    Err(err) => return Poll::Ready(Err(err)),
                },
                OpenState::Opening(mut future) => match future.as_mut().poll(cx) {
                    Poll::Ready(result) => return Poll::Ready(result),
                    Poll::Pending => {
                        self.state = OpenState::Opening(future);
                        return Poll::Pending;
                    }
                },
                OpenState::Finished => panic!("DataChannelOpenFuture polled after completion"),
            }
        }
    }
}

impl OpenInit {
    fn into_opening_future(self) -> Result<BoxOpenFuture> {
        let guard = self
            .registry
            .try_acquire(
                self.peer_id,
                self.protocol.clone(),
                DataChannelDirection::Outbound,
            )
            .ok_or(Error::DataChannel(
                DataChannelCloseReason::ResourceLimitExceeded,
            ))?;

        Ok(Box::pin(open_stream_with_guard(
            self.control,
            self.peer_id,
            self.protocol,
            self.open_timeout,
            guard,
        )))
    }
}

async fn open_stream_with_guard(
    mut control: Control,
    peer_id: PeerId,
    protocol: StreamProtocol,
    open_timeout: Duration,
    guard: ChannelGuard,
) -> Result<DataChannel> {
    let id = DataChannelId::next();
    let stream =
        match tokio::time::timeout(open_timeout, control.open_stream(peer_id, protocol.clone()))
            .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return Err(map_open_stream_error(e)),
            Err(_elapsed) => return Err(Error::Network(NetworkFailureKind::Timeout)),
        };

    Ok(DataChannel::new(
        id,
        peer_id,
        protocol,
        DataChannelDirection::Outbound,
        stream,
        Some(guard),
    ))
}

/// 将 `libp2p-stream` 的 `OpenStreamError` 归一化为 typed close reason。
fn map_open_stream_error(err: OpenStreamError) -> Error {
    match err {
        OpenStreamError::UnsupportedProtocol(_) => {
            Error::DataChannel(DataChannelCloseReason::UnsupportedProtocol)
        }
        OpenStreamError::Io(e) => Error::DataChannel(DataChannelCloseReason::Io(e.to_string())),
        // OpenStreamError 标了 #[non_exhaustive]，未知变体归一化为 Unknown。
        _ => Error::Network(NetworkFailureKind::Unknown),
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use super::*;
    use crate::data_channel::DataChannelLimits;

    fn test_peer() -> PeerId {
        libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id()
    }

    fn proto() -> StreamProtocol {
        StreamProtocol::new("/test/data/1")
    }

    fn control() -> Control {
        libp2p_stream::Behaviour::new().new_control()
    }

    fn single_outbound_registry() -> ChannelRegistry {
        ChannelRegistry::new(DataChannelLimits {
            max_inbound_per_peer: 4,
            max_outbound_per_peer: 1,
            max_per_protocol: 64,
        })
    }

    #[tokio::test]
    async fn zero_limit_returns_typed_error() {
        let registry = ChannelRegistry::new(DataChannelLimits {
            max_inbound_per_peer: 0,
            max_outbound_per_peer: 0,
            max_per_protocol: 0,
        });

        let err = DataChannelOpenFuture::new(
            test_peer(),
            proto(),
            control(),
            registry,
            Duration::from_secs(1),
        )
        .await
        .expect_err("zero limit should reject immediately");

        assert!(
            matches!(
                err,
                Error::DataChannel(DataChannelCloseReason::ResourceLimitExceeded)
            ),
            "actual error: {err:?}"
        );
    }

    #[test]
    fn future_is_lazy_before_first_poll() {
        let registry = single_outbound_registry();
        let peer = test_peer();
        let protocol = proto();
        let future = DataChannelOpenFuture::new(
            peer,
            protocol.clone(),
            control(),
            registry.clone(),
            Duration::from_secs(1),
        );

        let guard = registry.try_acquire(peer, protocol, DataChannelDirection::Outbound);
        assert!(guard.is_some(), "unpolled future should not acquire quota");

        drop(guard);
        drop(future);
    }

    #[tokio::test]
    async fn dropping_pending_open_releases_quota() {
        let registry = single_outbound_registry();
        let peer = test_peer();
        let protocol = proto();
        let mut future = Box::pin(DataChannelOpenFuture::new(
            peer,
            protocol.clone(),
            control(),
            registry.clone(),
            Duration::from_secs(60),
        ));

        assert!(matches!(futures::poll!(&mut future), Poll::Pending));
        assert!(
            registry
                .try_acquire(peer, protocol.clone(), DataChannelDirection::Outbound)
                .is_none(),
            "pending open should hold quota"
        );

        drop(future);

        let guard = registry.try_acquire(peer, protocol, DataChannelDirection::Outbound);
        assert!(
            guard.is_some(),
            "dropping pending open should release quota"
        );
    }
}
