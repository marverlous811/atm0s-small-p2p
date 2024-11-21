//! Task runner for a single connection
//! This must ensure not blocking by other actor.
//! We have some strict rules
//!
//! - Only use async with current connection stream
//! - For other communication should use try_send for avoiding blocking

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use futures::{SinkExt, StreamExt};
use quinn::{Connection, RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use tokio::{
    io::copy_bidirectional,
    select,
    sync::mpsc::{channel, Receiver, Sender},
    time::Interval,
};
use tokio_util::codec::Framed;

use crate::{
    ctx::SharedCtx,
    msg::{P2pServiceId, PeerMessage, StreamConnectReq, StreamConnectRes},
    router::RouteAction,
    stream::{wait_object, write_object, BincodeCodec, P2pQuicStream},
    utils::ErrorExt,
    ConnectionId, InternalEvent, P2pServiceEvent, PeerId, PeerMainData,
};

use super::PeerConnectionControl;

pub enum InternalStreamEvent {
    Add,
    Drop(u64),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PeerMetrics {
    pub uptime: u64,
    pub rtt: u16,
    pub stream_cnt: usize,
    pub lost_pkt: u64,
    pub lost_bytes: u64,
    pub send_bytes: u64,
    pub recv_bytes: u64,
}

pub struct PeerConnectionInternal {
    conn_id: ConnectionId,
    to_id: PeerId,
    ctx: SharedCtx,
    remote: SocketAddr,
    connection: Connection,
    framed: Framed<P2pQuicStream, BincodeCodec<PeerMessage>>,
    internal_tx: Sender<InternalEvent>,
    control_rx: Receiver<PeerConnectionControl>,
    stream_event_tx: Sender<InternalStreamEvent>,
    stream_event_rx: Receiver<InternalStreamEvent>,
    stream_cnt: usize,
    ticker: Interval,
    started: Instant,
}

impl PeerConnectionInternal {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: SharedCtx,
        conn_id: ConnectionId,
        to_id: PeerId,
        connection: Connection,
        main_send: SendStream,
        main_recv: RecvStream,
        internal_tx: Sender<InternalEvent>,
        control_rx: Receiver<PeerConnectionControl>,
    ) -> Self {
        let (tx, rx) = channel(100);
        let stream = P2pQuicStream::new(main_recv, main_send, tx.clone());

        Self {
            conn_id,
            to_id,
            ctx,
            remote: connection.remote_address(),
            connection,
            framed: Framed::new(stream, BincodeCodec::default()),
            internal_tx,
            control_rx,
            stream_event_rx: rx,
            stream_event_tx: tx,
            stream_cnt: 0,
            ticker: tokio::time::interval(Duration::from_secs(1)),
            started: Instant::now(),
        }
    }

    pub async fn run_loop(&mut self) -> anyhow::Result<()> {
        loop {
            select! {
                _ = self.ticker.tick() => {
                    let rtt_ms = self.connection.rtt().as_millis().min(u16::MAX as u128) as u16;
                    let connection_stats = self.connection.stats();
                    self.ctx.router().set_direct(self.conn_id, self.to_id, rtt_ms);
                    let metrics = PeerMetrics {
                        uptime: self.started.elapsed().as_secs(),
                        stream_cnt: self.stream_cnt,
                        lost_pkt: connection_stats.path.lost_packets,
                        lost_bytes: connection_stats.path.lost_bytes,
                        rtt: rtt_ms,
                        send_bytes: connection_stats.udp_tx.bytes,
                        recv_bytes: connection_stats.udp_rx.bytes,
                    };
                    let _ = self.internal_tx.try_send(InternalEvent::PeerStats(self.conn_id, self.to_id, metrics));
                },
                open = self.connection.accept_bi() => {
                    let (send, recv) = open?;
                    self.on_accept_bi(send, recv).await?;
                },
                frame = self.framed.next() => {
                    let msg = frame.ok_or(anyhow!("peer main stream ended"))??;
                    self.on_msg(msg).await?;
                },
                control = self.control_rx.recv() => {
                    let control = control.ok_or(anyhow!("peer control channel ended"))?;
                    self.on_control(control).await?;
                },
                stream_ev = self.stream_event_rx.recv() => {
                    if let Some(ev) = stream_ev {
                        match ev {
                            InternalStreamEvent::Add => {
                                self.stream_cnt += 1;
                                log::debug!("on new stream created...");
                            }
                            InternalStreamEvent::Drop(duration) => {
                                self.stream_cnt -= 1;
                                log::debug!("on stream is dropped... lifetime {}", duration);
                            }
                        }
                    }
                }
            }
        }
    }

    async fn on_accept_bi(&mut self, send: SendStream, recv: RecvStream) -> anyhow::Result<()> {
        log::info!("[PeerConnectionInternal {}] on new bi", self.remote);
        let stream = P2pQuicStream::new(recv, send, self.stream_event_tx.clone());
        tokio::spawn(accept_bi(self.to_id, stream, self.ctx.clone()));
        Ok(())
    }

    async fn on_control(&mut self, control: PeerConnectionControl) -> anyhow::Result<()> {
        match control {
            PeerConnectionControl::Send(item) => Ok(self.framed.send(item).await?),
            PeerConnectionControl::OpenStream(service, source, dest, meta, tx) => {
                let remote = self.remote;
                let connection = self.connection.clone();
                let stream_event_tx = self.stream_event_tx.clone();
                tokio::spawn(async move {
                    log::info!("[PeerConnectionInternal {remote}] open_bi for service {service}");
                    let res = open_bi(connection, source, dest, service, meta, stream_event_tx).await;
                    if let Err(e) = &res {
                        log::error!("[PeerConnectionInternal {remote}] open_bi for service {service} error {e}");
                    } else {
                        log::info!("[PeerConnectionInternal {remote}] open_bi for service {service} success");
                    }
                    tx.send(res).map_err(|_| "internal channel error").print_on_err("[PeerConnectionInternal] answer open_bi");
                });
                Ok(())
            }
        }
    }

    async fn on_msg(&mut self, msg: PeerMessage) -> anyhow::Result<()> {
        match msg {
            PeerMessage::Sync { route, advertise } => {
                if let Err(_e) = self.internal_tx.try_send(InternalEvent::PeerData(self.conn_id, self.to_id, PeerMainData::Sync { route, advertise })) {
                    log::warn!("[PeerConnectionInternal {}] queue main loop full", self.remote);
                }
            }
            PeerMessage::Broadcast(source, service_id, msg_id, data) => {
                if self.ctx.check_broadcast_msg(msg_id) {
                    for conn in self.ctx.conns().into_iter().filter(|p| !self.to_id.eq(&p.to_id())) {
                        conn.try_send(PeerMessage::Broadcast(source, service_id, msg_id, data.clone()))
                            .print_on_err("[PeerConnectionInternal] broadcast data over peer alias");
                    }

                    if let Some(service) = self.ctx.get_service(&service_id) {
                        log::debug!("[PeerConnectionInternal {}] broadcast msg {msg_id} to service {service_id}", self.remote);
                        service.try_send(P2pServiceEvent::Broadcast(source, data)).print_on_err("[PeerConnectionInternal] send service msg");
                    } else {
                        log::warn!("[PeerConnectionInternal {}] broadcast msg to unknown service {service_id}", self.remote);
                    }
                } else {
                    log::debug!("[PeerConnectionInternal {}] broadcast msg {msg_id} already deliveried", self.remote);
                }
            }
            PeerMessage::Unicast(source, dest, service_id, data) => match self.ctx.router().action(&dest) {
                Some(RouteAction::Local) => {
                    if let Some(service) = self.ctx.get_service(&service_id) {
                        service.try_send(P2pServiceEvent::Unicast(source, data)).print_on_err("[PeerConnectionInternal] send service msg");
                    } else {
                        log::warn!("[PeerConnectionInternal {}] service {service_id} not found", self.remote);
                    }
                }
                Some(RouteAction::Next(next)) => {
                    if let Some(conn) = self.ctx.conn(&next) {
                        conn.try_send(PeerMessage::Unicast(source, dest, service_id, data))
                            .print_on_err("[PeerConnectionInternal] send data over peer alias");
                    } else {
                        log::warn!("[PeerConnectionInternal {}] peer {next} not found", self.remote);
                    }
                }
                None => {
                    log::warn!("[PeerConnectionInternal {}] path to {dest} not found", self.remote);
                }
            },
        }
        Ok(())
    }
}

async fn open_bi(connection: Connection, source: PeerId, dest: PeerId, service: P2pServiceId, meta: Vec<u8>, stream_tx: Sender<InternalStreamEvent>) -> anyhow::Result<P2pQuicStream> {
    let (send, recv) = connection.open_bi().await?;
    let mut stream = P2pQuicStream::new(recv, send, stream_tx);
    write_object::<_, _, 500>(&mut stream, &StreamConnectReq { source, dest, service, meta }).await?;
    let res = wait_object::<_, StreamConnectRes, 500>(&mut stream).await?;
    res.map(|_| stream).map_err(|e| anyhow!("{e}"))
}

async fn accept_bi(to_peer: PeerId, mut stream: P2pQuicStream, ctx: SharedCtx) -> anyhow::Result<()> {
    let req = wait_object::<_, StreamConnectReq, 500>(&mut stream).await?;
    let StreamConnectReq { dest, source, service, meta } = req;
    match ctx.router().action(&dest) {
        Some(RouteAction::Local) => {
            if let Some(service_tx) = ctx.get_service(&service) {
                log::info!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} => process local");
                write_object::<_, _, 500>(&mut stream, &Ok::<_, String>(())).await?;
                service_tx
                    .send(P2pServiceEvent::Stream(source, meta, stream))
                    .await
                    .print_on_err("[PeerConnectionInternal] send accepted stream to service");
                Ok(())
            } else {
                log::warn!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} => service not found");
                write_object::<_, _, 500>(&mut stream, &Err::<(), _>("service not found".to_string())).await?;
                Err(anyhow!("service not found"))
            }
        }
        Some(RouteAction::Next(next)) => {
            if let Some(alias) = ctx.conn(&next) {
                log::info!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} => forward to {next}");
                match alias.open_stream(service, source, dest, meta).await {
                    Ok(mut next_stream) => {
                        write_object::<_, _, 500>(&mut stream, &Ok::<_, String>(())).await?;
                        log::info!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} => start copy_bidirectional");
                        match copy_bidirectional(&mut next_stream, &mut stream).await {
                            Ok(stats) => {
                                log::info!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} done {stats:?}");
                            }
                            Err(err) => {
                                log::error!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} err {err}");
                            }
                        }
                        Ok(())
                    }
                    Err(err) => {
                        log::error!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} => open bi error {err}");
                        write_object::<_, _, 500>(&mut stream, &Err::<(), _>(err.to_string())).await?;
                        Err(err)
                    }
                }
            } else {
                log::warn!("[PeerConnectionInternal {to_peer}] new stream with service {service} source {source} to dest {dest} => but connection for next {next} not found");
                write_object::<_, _, 500>(&mut stream, &Err::<(), _>("route not found".to_string())).await?;
                Err(anyhow!("route not found"))
            }
        }
        None => {
            log::warn!("[PeerConnectionInternal {to_peer}] new stream with service {service} source {source} to dest {dest} => but route path not found");
            write_object::<_, _, 500>(&mut stream, &Err::<(), _>("route not found".to_string())).await?;
            Err(anyhow!("route not found"))
        }
    }
}
