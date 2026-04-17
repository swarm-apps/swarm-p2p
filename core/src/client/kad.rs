use libp2p::kad::{Record, RecordKey};

use super::future::CommandFuture;
use crate::Result;
use crate::command::{
    BootstrapCommand, BootstrapResult, GetClosestPeersCommand, GetClosestPeersResult,
    GetProvidersCommand, GetProvidersResult, GetRecordCommand, GetRecordResult, PutRecordCommand,
    RemoveRecordCommand, StartProvideCommand, StopProvideCommand,
};
use crate::runtime::CborMessage;
use crate::util::QueryStatsInfo;

use super::NetClient;

impl<Req, Resp> NetClient<Req, Resp>
where
    Req: CborMessage,
    Resp: CborMessage,
{
    /// Bootstrap - 加入 DHT 网络，填充路由表
    pub async fn bootstrap(&self) -> Result<BootstrapResult> {
        let cmd = BootstrapCommand::new();
        CommandFuture::new(cmd, self.command_tx.clone()).await
    }

    /// 从 DHT 获取记录
    pub async fn get_record(&self, key: RecordKey) -> Result<GetRecordResult> {
        let cmd = GetRecordCommand::new(key);
        CommandFuture::new(cmd, self.command_tx.clone()).await
    }

    /// 将记录存入 DHT
    pub async fn put_record(&self, record: Record) -> Result<QueryStatsInfo> {
        let cmd = PutRecordCommand::new(record);
        CommandFuture::new(cmd, self.command_tx.clone()).await
    }

    /// 从 DHT 获取 Provider 列表
    pub async fn get_providers(&self, key: RecordKey) -> Result<GetProvidersResult> {
        let cmd = GetProvidersCommand::new(key);
        CommandFuture::new(cmd, self.command_tx.clone()).await
    }

    /// 查找最近的 Peers
    pub async fn get_closest_peers(&self, key: RecordKey) -> Result<GetClosestPeersResult> {
        let cmd = GetClosestPeersCommand::new(key);
        CommandFuture::new(cmd, self.command_tx.clone()).await
    }

    /// 开始提供资源
    pub async fn start_provide(&self, key: RecordKey) -> Result<QueryStatsInfo> {
        let cmd = StartProvideCommand::new(key);
        CommandFuture::new(cmd, self.command_tx.clone()).await
    }

    /// 停止提供资源
    pub async fn stop_provide(&self, key: RecordKey) -> Result<()> {
        let cmd = StopProvideCommand::new(key);
        CommandFuture::new(cmd, self.command_tx.clone()).await
    }

    /// 从本地存储中删除记录
    pub async fn remove_record(&self, key: RecordKey) -> Result<()> {
        let cmd = RemoveRecordCommand::new(key);
        CommandFuture::new(cmd, self.command_tx.clone()).await
    }
}
