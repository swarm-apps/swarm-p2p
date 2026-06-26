use async_trait::async_trait;
use libp2p::PeerId;
use libp2p::kad::{self, RecordKey};
use libp2p::swarm::SwarmEvent;
use tracing::{error, info};

use crate::error::Error;
use crate::runtime::{CborMessage, CoreBehaviourEvent};
use crate::util::QueryStatsInfo;

use super::super::{CommandHandler, CoreSwarm, OnEventResult, ResultHandle};

/// GetProviders 命令结果
#[derive(Debug, Clone)]
pub struct GetProvidersResult {
    /// 找到的 Provider PeerId 列表
    pub providers: Vec<PeerId>,
    /// 查询统计信息
    pub stats: QueryStatsInfo,
}

pub struct GetProvidersCommand {
    key: RecordKey,
    query_id: Option<kad::QueryId>,
    providers: Vec<PeerId>,
    stats: Option<kad::QueryStats>,
}

impl GetProvidersCommand {
    pub fn new(key: RecordKey) -> Self {
        Self {
            key,
            query_id: None,
            providers: Vec::new(),
            stats: None,
        }
    }
}

#[async_trait]
impl<Req: CborMessage, Resp: CborMessage> CommandHandler<Req, Resp> for GetProvidersCommand {
    type Result = GetProvidersResult;

    async fn run(
        &mut self,
        swarm: &mut CoreSwarm<Req, Resp>,
        _handle: &ResultHandle<Self::Result>,
    ) {
        let query_id = swarm.behaviour_mut().kad.get_providers(self.key.clone());
        self.query_id = Some(query_id);
    }

    async fn on_event(
        &mut self,
        event: SwarmEvent<CoreBehaviourEvent<Req, Resp>>,
        handle: &ResultHandle<Self::Result>,
    ) -> OnEventResult<Req, Resp> {
        match event {
            SwarmEvent::Behaviour(CoreBehaviourEvent::Kad(
                kad::Event::OutboundQueryProgressed {
                    id,
                    result: kad::QueryResult::GetProviders(res),
                    stats,
                    step,
                },
            )) if self.query_id == Some(id) => {
                // 累积统计
                super::merge_stats(&mut self.stats, stats);

                // 处理结果
                match res {
                    Ok(kad::GetProvidersOk::FoundProviders { providers, .. }) => {
                        // 收集 providers
                        self.providers.extend(providers);
                        info!(
                            "GetProviders progress: found {} providers so far",
                            self.providers.len()
                        );
                    }
                    Ok(kad::GetProvidersOk::FinishedWithNoAdditionalRecord { closest_peers }) => {
                        // 查询结束，closest_peers 是最近的节点（不一定是 provider）
                        info!(
                            "GetProviders finished, {} closest peers",
                            closest_peers.len()
                        );
                    }
                    Err(e) => {
                        error!("GetProviders error: {:?}", e);
                        handle.finish(Err(Error::Kad(format!("GetProviders: {:?}", e))));
                        return (false, None); // 消费，完成
                    }
                }

                // 非最后一步，继续等待
                if !step.last {
                    return (true, None); // 消费，继续等待
                }

                // 查询完成
                let stats_info = QueryStatsInfo::from(self.stats.as_ref().unwrap());
                info!(
                    "GetProviders completed: {} providers, {:?}",
                    self.providers.len(),
                    stats_info
                );

                handle.finish(Ok(GetProvidersResult {
                    providers: std::mem::take(&mut self.providers),
                    stats: stats_info,
                }));

                (false, None) // 消费，完成
            }
            other => (true, Some(other)), // 继续等待
        }
    }
}
