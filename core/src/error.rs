use libp2p::noise;
use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Noise authentication error: {0}")]
    Noise(#[from] noise::Error),

    #[error("IO error: {0}")]
    Io(io::Error),

    #[error("Dial error: {0}")]
    Dial(String),

    #[error("Listen error: {0}")]
    Listen(String),

    #[error("Kad error: {0}")]
    Kad(String),

    #[error("Request-response error: {0}")]
    RequestResponse(String),

    #[error("Behaviour error: {0}")]
    Behaviour(String),

    #[error("Network failure: {0}")]
    Network(NetworkFailureKind),

    #[error("Data channel: {0}")]
    DataChannel(crate::data_channel::DataChannelCloseReason),
}

/// Typed 网络失败分类。
///
/// 替换 / 补充 request-response 的字符串错误，供下游状态机准确分类
/// （timeout / dial / unsupported / closed / cancelled / codec / limit）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkFailureKind {
    /// 请求超时。
    Timeout,
    /// 拨号失败（无法建立连接）。
    DialFailure,
    /// 对端不支持该协议。
    UnsupportedProtocol,
    /// 连接在请求完成前关闭。
    ConnectionClosed,
    /// 被本地取消。
    Cancelled,
    /// 编解码错误（CBOR 序列化 / 反序列化）。
    CodecError,
    /// 超出资源限制。
    ResourceLimitExceeded,
    /// 其它 / 未知失败。
    Unknown,
}

impl std::fmt::Display for NetworkFailureKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Timeout => "timeout",
            Self::DialFailure => "dial failure",
            Self::UnsupportedProtocol => "unsupported protocol",
            Self::ConnectionClosed => "connection closed",
            Self::Cancelled => "cancelled",
            Self::CodecError => "codec error",
            Self::ResourceLimitExceeded => "resource limit exceeded",
            Self::Unknown => "unknown",
        };
        f.write_str(s)
    }
}
