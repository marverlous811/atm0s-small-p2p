use std::{collections::VecDeque, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::{select, time::Interval};

use crate::{peer::PeerMetrics, ConnectionId, ErrorExt, P2pServiceEvent, PeerId};

use super::P2pService;

#[derive(Debug, PartialEq, Eq)]
pub enum MetricsServiceEvent {
    PeerMetrics(PeerId, Vec<(ConnectionId, PeerId, PeerMetrics)>),
}

#[derive(Deserialize, Serialize)]
enum Message {
    Scan,
    Info(Vec<(ConnectionId, PeerId, PeerMetrics)>),
}

pub struct MetricsService {
    is_collector: bool,
    service: P2pService,
    ticker: Interval,
    outs: VecDeque<MetricsServiceEvent>,
}

impl MetricsService {
    pub fn new(collect_interval: Option<Duration>, service: P2pService, is_collector: bool) -> Self {
        let ticker = tokio::time::interval(collect_interval.unwrap_or(Duration::from_secs(1)));

        Self {
            is_collector,
            ticker,
            service,
            outs: VecDeque::new(),
        }
    }

    pub async fn recv(&mut self) -> anyhow::Result<MetricsServiceEvent> {
        loop {
            if let Some(out) = self.outs.pop_front() {
                return Ok(out);
            }

            select! {
                _ = self.ticker.tick() => {
                    if self.is_collector {
                        let metrics = self.service.ctx.metrics();
                        self.outs.push_back(MetricsServiceEvent::PeerMetrics(self.service.router().local_id(), metrics));

                        let requester = self.service.requester();
                        tokio::spawn(async move {
                            requester.send_broadcast(bincode::serialize(&Message::Scan).expect("should convert to buf")).await;
                        });
                    }
                }
                event = self.service.recv() => match event.expect("should work") {
                    P2pServiceEvent::Unicast(from, data) | P2pServiceEvent::Broadcast(from, data) => {
                        if let Ok(msg) = bincode::deserialize::<Message>(&data) {
                            match msg {
                                Message::Scan => {
                                    let metrics = self.service.ctx.metrics();
                                    let requester = self.service.requester();
                                    tokio::spawn(async move {
                                        requester.send_unicast(from, bincode::serialize(&Message::Info(metrics)).expect("should convert to buf"))
                                            .await
                                            .print_on_err("send metrics info to collector error");
                                    });
                                }
                                Message::Info(peer_metrics) => {
                                    self.outs.push_back(MetricsServiceEvent::PeerMetrics(from, peer_metrics));
                                }
                            }
                        }
                    }
                    P2pServiceEvent::Stream(..) => {}
                }
            }
        }
    }
}
