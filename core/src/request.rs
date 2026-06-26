//! Request-response 操作的 per-call 选项。

use std::time::Duration;

/// Request-response 单次调用选项。
///
/// 不提供任何字段时退化为全局默认行为（使用 `NodeConfig::req_resp_timeout`）。
#[derive(Debug, Clone, Default)]
pub struct RequestOptions {
    /// 覆盖本次请求的超时（`None` 用全局默认）。
    pub timeout: Option<Duration>,
    /// 关联元信息，透传供调用方做日志 / tracing 关联。
    pub correlation: Option<String>,
}

impl RequestOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置本次请求的自定义超时。
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// 设置关联元信息。
    pub fn with_correlation(mut self, correlation: impl Into<String>) -> Self {
        self.correlation = Some(correlation.into());
        self
    }
}
