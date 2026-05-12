use std::error::Error;
use std::net::SocketAddr;

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{debug, info, warn};

type DynError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug)]
struct DriverMessage {
    slot: u16,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct DriverWriteRequest {
    slot: u16,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct UdpSendRequest {
    peer: SocketAddr,
    payload: Vec<u8>,
}

#[derive(Debug)]
enum SessionEvent {
    DriverMessage(DriverMessage),
    UdpPacket { peer: SocketAddr, payload: Vec<u8> },
}

#[derive(Debug, Clone, Copy)]
enum ServerState {
    WaitingForClient,
    ConnectedToClient { client_addr: SocketAddr },
}

/// Tiny mock of a callback-based driver that periodically emits messages.
struct MockDriver;

impl MockDriver {
    fn spawn_event_source(tx: mpsc::Sender<SessionEvent>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(700));
            let mut seq: u32 = 0;

            loop {
                ticker.tick().await;
                seq += 1;

                // Simulate callback: "new_message_event(slot, bytes)"
                let slot = (seq % 4) as u16;
                let payload = format!("driver-msg-{seq}").into_bytes();
                let msg = SessionEvent::DriverMessage(DriverMessage { slot, payload });

                if tx.send(msg).await.is_err() {
                    warn!("session channel closed, stopping mock driver event source");
                    break;
                }
            }
        })
    }

    fn spawn_writer(mut rx: mpsc::Receiver<DriverWriteRequest>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                // In a real backend this would call driver.write(slot, payload)
                info!(
                    slot = req.slot,
                    bytes = req.payload.len(),
                    payload = String::from_utf8_lossy(&req.payload).as_ref(),
                    "mock driver write()"
                );
            }
            warn!("driver writer channel closed");
        })
    }
}

fn spawn_udp_receiver(
    bind_addr: SocketAddr,
    session_tx: mpsc::Sender<SessionEvent>,
) -> tokio::task::JoinHandle<Result<(), DynError>> {
    tokio::spawn(async move {
        let socket = UdpSocket::bind(bind_addr).await?;
        info!(%bind_addr, "udp receiver bound");

        let mut buf = vec![0_u8; 2048];
        loop {
            let (len, peer) = socket.recv_from(&mut buf).await?;
            let payload = buf[..len].to_vec();
            debug!(%peer, bytes = payload.len(), "udp datagram received");

            if session_tx
                .send(SessionEvent::UdpPacket { peer, payload })
                .await
                .is_err()
            {
                warn!("session channel closed, stopping udp receiver task");
                break;
            }
        }
        Ok(())
    })
}

fn spawn_session_state_machine(
    mut session_rx: mpsc::Receiver<SessionEvent>,
    udp_tx: mpsc::Sender<UdpSendRequest>,
    driver_tx: mpsc::Sender<DriverWriteRequest>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut state = ServerState::WaitingForClient;

        while let Some(event) = session_rx.recv().await {
            match event {
                SessionEvent::UdpPacket { peer, payload } => {
                    match state {
                        ServerState::WaitingForClient => {
                            state = ServerState::ConnectedToClient { client_addr: peer };
                            info!(%peer, "state transition: WaitingForClient -> ConnectedToClient");
                        }
                        ServerState::ConnectedToClient { client_addr } if client_addr != peer => {
                            warn!(
                                %peer,
                                expected = %client_addr,
                                "ignoring packet from non-active client"
                            );
                            continue;
                        }
                        ServerState::ConnectedToClient { .. } => {}
                    }

                    let slot = 0_u16;
                    info!(
                        %peer,
                        slot,
                        bytes = payload.len(),
                        payload = String::from_utf8_lossy(&payload).as_ref(),
                        "udp -> driver"
                    );

                    if driver_tx
                        .send(DriverWriteRequest { slot, payload })
                        .await
                        .is_err()
                    {
                        warn!("driver write channel closed, stopping session state machine");
                        break;
                    }
                }
                SessionEvent::DriverMessage(msg) => match state {
                    ServerState::WaitingForClient => {
                        debug!(
                            slot = msg.slot,
                            bytes = msg.payload.len(),
                            "dropping driver message while waiting for client"
                        );
                    }
                    ServerState::ConnectedToClient { client_addr } => {
                        info!(
                            peer = %client_addr,
                            slot = msg.slot,
                            bytes = msg.payload.len(),
                            payload = String::from_utf8_lossy(&msg.payload).as_ref(),
                            "driver -> udp"
                        );

                        if udp_tx
                            .send(UdpSendRequest {
                                peer: client_addr,
                                payload: msg.payload,
                            })
                            .await
                            .is_err()
                        {
                            warn!("udp send channel closed, stopping session state machine");
                            break;
                        }
                    }
                },
            }
        }

        warn!("session state machine stopped");
    })
}

fn spawn_udp_sender(
    bind_addr: SocketAddr,
    mut rx: mpsc::Receiver<UdpSendRequest>,
) -> tokio::task::JoinHandle<Result<(), DynError>> {
    tokio::spawn(async move {
        let socket = UdpSocket::bind(bind_addr).await?;
        info!(%bind_addr, "udp sender ready");

        while let Some(req) = rx.recv().await {
            let sent = socket.send_to(&req.payload, req.peer).await?;
            debug!(sent, peer = %req.peer, "udp datagram sent");
        }

        warn!("udp sender channel closed");
        Ok(())
    })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), DynError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tokio_udp_demo=debug".into()),
        )
        .with_target(false)
        .compact()
        .init();

    // Simple local loopback config for prototyping.
    let app_udp_recv_addr: SocketAddr = "127.0.0.1:7001".parse()?;
    let app_udp_send_bind_addr: SocketAddr = "127.0.0.1:0".parse()?;

    // Bounded channels to keep latency predictable and provide backpressure.
    let (session_tx, session_rx) = mpsc::channel::<SessionEvent>(256);
    let (udp_tx, udp_rx) = mpsc::channel::<UdpSendRequest>(256);
    let (udp_to_driver_tx, udp_to_driver_rx) = mpsc::channel::<DriverWriteRequest>(256);

    let _driver_event_task = MockDriver::spawn_event_source(session_tx.clone());
    let _session_task = spawn_session_state_machine(session_rx, udp_tx, udp_to_driver_tx.clone());
    let _driver_writer_task = MockDriver::spawn_writer(udp_to_driver_rx);
    let udp_recv_task = spawn_udp_receiver(app_udp_recv_addr, session_tx);
    let _udp_send_task = spawn_udp_sender(app_udp_send_bind_addr, udp_rx);

    info!(
        recv_addr = %app_udp_recv_addr,
        "bridge started in WaitingForClient mode; first UDP sender becomes active client"
    );

    udp_recv_task.await??;
    Ok(())
}
