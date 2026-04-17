use async_trait::async_trait;
use libp2p::PeerId;
use libp2p::kad::{self, RecordKey};
use libp2p::swarm::SwarmEvent;
use tracing::{error, info};

use crate::error::Error;
use crate::runtime::{CborMessage, CoreBehaviourEvent};
use crate::util::QueryStatsInfo;

use super::super::{CommandHandler, CoreSwarm, OnEventResult, ResultHandle};

/// GetClosestPeers 命令结果
#[derive(Debug, Clone)]
pub struct GetClosestPeersResult {
    /// 最近的 PeerId 列表
    pub peers: Vec<PeerId>,
    /// 查询统计信息
    pub stats: QueryStatsInfo,
}

pub struct GetClosestPeersCommand {
    key: RecordKey,
    query_id: Option<kad::QueryId>,
    peers: Vec<PeerId>,
    stats: Option<kad::QueryStats>,
}

impl GetClosestPeersCommand {
    pub fn new(key: RecordKey) -> Self {
        Self {
            key,
            query_id: None,
            peers: Vec::new(),
            stats: None,
        }
    }
}

#[async_trait]
impl<Req: CborMessage, Resp: CborMessage> CommandHandler<Req, Resp> for GetClosestPeersCommand {
    type Result = GetClosestPeersResult;

    async fn run(
        &mut self,
        swarm: &mut CoreSwarm<Req, Resp>,
        _handle: &ResultHandle<Self::Result>,
    ) {
        let query_id = swarm
            .behaviour_mut()
            .kad
            .get_closest_peers(self.key.to_vec());
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
                    result: kad::QueryResult::GetClosestPeers(res),
                    stats,
                    step,
                },
            )) if self.query_id == Some(id) => {
                // 累积统计
                super::merge_stats(&mut self.stats, stats);

                // 处理结果
                match res {
                    Ok(ok) => {
                        // 收集最近的 peers (从 PeerInfo 中提取 PeerId)
                        self.peers.extend(ok.peers.iter().map(|p| p.peer_id));
                        info!(
                            "GetClosestPeers progress: found {} peers so far",
                            self.peers.len()
                        );
                    }
                    Err(e) => {
                        error!("GetClosestPeers error: {:?}", e);
                        handle.finish(Err(Error::Kad(format!("GetClosestPeers: {:?}", e))));
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
                    "GetClosestPeers completed: {} peers, {:?}",
                    self.peers.len(),
                    stats_info
                );

                handle.finish(Ok(GetClosestPeersResult {
                    peers: std::mem::take(&mut self.peers),
                    stats: stats_info,
                }));

                (false, None) // 消费，完成
            }
            other => (true, Some(other)), // 继续等待
        }
    }
}
