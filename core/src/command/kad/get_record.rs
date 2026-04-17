use async_trait::async_trait;
use libp2p::kad::{self, Record, RecordKey};
use libp2p::swarm::SwarmEvent;
use tracing::{error, info};

use crate::error::Error;
use crate::runtime::{CborMessage, CoreBehaviourEvent};
use crate::util::QueryStatsInfo;

use super::super::{CommandHandler, CoreSwarm, OnEventResult, ResultHandle};

/// GetRecord 命令结果
#[derive(Debug, Clone)]
pub struct GetRecordResult {
    /// 找到的记录
    pub record: Record,
    /// 查询统计信息
    pub stats: QueryStatsInfo,
}

pub struct GetRecordCommand {
    key: RecordKey,
    query_id: Option<kad::QueryId>,
    record: Option<Record>,
    stats: Option<kad::QueryStats>,
}

impl GetRecordCommand {
    pub fn new(key: RecordKey) -> Self {
        Self {
            key,
            query_id: None,
            record: None,
            stats: None,
        }
    }
}

#[async_trait]
impl<Req: CborMessage, Resp: CborMessage> CommandHandler<Req, Resp> for GetRecordCommand {
    type Result = GetRecordResult;

    async fn run(
        &mut self,
        swarm: &mut CoreSwarm<Req, Resp>,
        _handle: &ResultHandle<Self::Result>,
    ) {
        let query_id = swarm.behaviour_mut().kad.get_record(self.key.clone());
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
                    result: kad::QueryResult::GetRecord(res),
                    stats,
                    step,
                },
            )) if self.query_id == Some(id) => {
                // 累积统计
                super::merge_stats(&mut self.stats, stats);

                // 处理结果
                match res {
                    Ok(ok) => {
                        // 保存找到的记录（取第一个）
                        if self.record.is_none()
                            && let kad::GetRecordOk::FoundRecord(peer_record) = ok
                        {
                            self.record = Some(peer_record.record);
                            info!("GetRecord: found record");
                        }
                    }
                    Err(e) => {
                        // 如果已经找到记录，忽略后续错误
                        if self.record.is_none() {
                            error!("GetRecord error: {:?}", e);
                            if step.last {
                                handle.finish(Err(Error::Kad(format!("GetRecord: {:?}", e))));
                                return (false, None); // 消费，完成
                            }
                        }
                    }
                }

                // 非最后一步，继续等待
                if !step.last {
                    return (true, None); // 消费，继续等待
                }

                // 查询完成
                let stats_info = QueryStatsInfo::from(self.stats.as_ref().unwrap());

                match self.record.take() {
                    Some(record) => {
                        info!("GetRecord completed: {:?}", stats_info);
                        handle.finish(Ok(GetRecordResult {
                            record,
                            stats: stats_info,
                        }));
                    }
                    None => {
                        handle.finish(Err(Error::Kad("Record not found".to_string())));
                    }
                }

                (false, None) // 消费，完成
            }
            other => (true, Some(other)), // 继续等待
        }
    }
}
