use async_trait::async_trait;
use libp2p::kad::{self, Record};
use libp2p::swarm::SwarmEvent;
use tracing::{error, info};

use crate::error::Error;
use crate::runtime::{CborMessage, CoreBehaviourEvent};
use crate::util::QueryStatsInfo;

use super::super::{CommandHandler, CoreSwarm, OnEventResult, ResultHandle};

pub struct PutRecordCommand {
    record: Record,
    query_id: Option<kad::QueryId>,
    stats: Option<kad::QueryStats>,
}

impl PutRecordCommand {
    pub fn new(record: Record) -> Self {
        Self {
            record,
            query_id: None,
            stats: None,
        }
    }
}

#[async_trait]
impl<Req: CborMessage, Resp: CborMessage> CommandHandler<Req, Resp> for PutRecordCommand {
    type Result = QueryStatsInfo;

    async fn run(&mut self, swarm: &mut CoreSwarm<Req, Resp>, handle: &ResultHandle<Self::Result>) {
        match swarm
            .behaviour_mut()
            .kad
            .put_record(self.record.clone(), kad::Quorum::One)
        {
            Ok(query_id) => {
                self.query_id = Some(query_id);
            }
            Err(e) => {
                handle.finish(Err(Error::Kad(format!("PutRecord store: {}", e))));
            }
        }
    }

    async fn on_event(
        &mut self,
        event: SwarmEvent<CoreBehaviourEvent<Req, Resp>>,
        _swarm: &mut CoreSwarm<Req, Resp>,
        handle: &ResultHandle<Self::Result>,
    ) -> OnEventResult<Req, Resp> {
        match event {
            SwarmEvent::Behaviour(CoreBehaviourEvent::Kad(
                kad::Event::OutboundQueryProgressed {
                    id,
                    result: kad::QueryResult::PutRecord(res),
                    stats,
                    step,
                },
            )) if self.query_id == Some(id) => {
                // 累积统计
                super::merge_stats(&mut self.stats, stats);

                // 非最后一步，继续等待
                if !step.last {
                    return (true, None); // 消费，继续等待
                }

                // 查询完成，处理结果
                let stats_info = QueryStatsInfo::from(self.stats.as_ref().unwrap());
                match res {
                    Ok(_) => {
                        info!("PutRecord success: {:?}", stats_info);
                        handle.finish(Ok(stats_info));
                    }
                    Err(e) => {
                        error!("PutRecord error: {:?}", e);
                        handle.finish(Err(Error::Kad(format!("PutRecord: {:?}", e))));
                    }
                }

                (false, None) // 消费，完成
            }
            other => (true, Some(other)), // 继续等待
        }
    }
}
