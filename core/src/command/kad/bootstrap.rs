use async_trait::async_trait;
use libp2p::kad::{self, QueryId};
use libp2p::swarm::SwarmEvent;
use tracing::{error, info};

use crate::error::Error;
use crate::runtime::{CborMessage, CoreBehaviourEvent};
use crate::util::QueryStatsInfo;

use super::super::{CommandHandler, CoreSwarm, OnEventResult, ResultHandle};

/// Bootstrap 命令结果
#[derive(Debug, Clone)]
pub struct BootstrapResult {
    /// 剩余待查询的节点数量
    pub num_remaining: u32,
    /// 查询统计信息
    pub stats: QueryStatsInfo,
}

/// Bootstrap 命令 - 加入 DHT 网络，填充路由表
pub struct BootstrapCommand {
    query_id: Option<QueryId>,
    stats: Option<kad::QueryStats>,
}

impl BootstrapCommand {
    pub fn new() -> Self {
        Self {
            query_id: None,
            stats: None,
        }
    }
}

impl Default for BootstrapCommand {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<Req: CborMessage, Resp: CborMessage> CommandHandler<Req, Resp> for BootstrapCommand {
    type Result = BootstrapResult;

    async fn run(&mut self, swarm: &mut CoreSwarm<Req, Resp>, handle: &ResultHandle<Self::Result>) {
        match swarm.behaviour_mut().kad.bootstrap() {
            Ok(query_id) => {
                self.query_id = Some(query_id);
                info!("Bootstrap started, query_id: {:?}", query_id);
            }
            Err(e) => {
                error!("Bootstrap failed to start: {:?}", e);
                handle.finish(Err(Error::Kad(
                    "Bootstrap failed: no known peers".to_string(),
                )));
            }
        }
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
                    result: kad::QueryResult::Bootstrap(res),
                    stats,
                    step,
                },
            )) if self.query_id == Some(id) => {
                // 累积统计
                super::merge_stats(&mut self.stats, stats);

                // 处理结果
                match res {
                    Ok(kad::BootstrapOk {
                        peer,
                        num_remaining,
                    }) => {
                        info!(
                            "Bootstrap progress: peer {:?}, {} remaining",
                            peer, num_remaining
                        );
                    }
                    Err(e) => {
                        error!("Bootstrap error: {:?}", e);
                        handle.finish(Err(Error::Kad(format!("Bootstrap: {:?}", e))));
                        return (false, None); // 消费，完成
                    }
                }

                // 非最后一步，继续等待
                if !step.last {
                    return (true, None); // 消费，继续等待
                }

                // Bootstrap 完成
                let stats_info = QueryStatsInfo::from(self.stats.as_ref().unwrap());
                info!("Bootstrap completed: {:?}", stats_info);

                // 获取最后一次的 num_remaining
                let num_remaining = match res {
                    Ok(kad::BootstrapOk { num_remaining, .. }) => num_remaining,
                    Err(_) => 0,
                };

                handle.finish(Ok(BootstrapResult {
                    num_remaining,
                    stats: stats_info,
                }));

                (false, None) // 消费，完成
            }
            other => (true, Some(other)), // 继续等待
        }
    }
}
