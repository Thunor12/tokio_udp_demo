use std::error::Error;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{self, Duration, Instant};
use tracing::{debug, info, warn};

type DynError = Box<dyn Error + Send + Sync + 'static>;

const BENCH_MESSAGE_SIZE: usize = 56;

#[derive(Debug)]
enum SessionEvent {
    ClientPacket { peer: SocketAddr },
    BenchMessage(Vec<u8>),
}

#[derive(Debug)]
struct UdpSendRequest {
    peer: SocketAddr,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
enum ServerState {
    WaitingForClient,
    ConnectedToClient {
        client_addr: SocketAddr,
        last_seen: Instant,
    },
}

fn unix_now_ns_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn make_bench_payload(seq: u64) -> [u8; BENCH_MESSAGE_SIZE] {
    let mut payload = [0_u8; BENCH_MESSAGE_SIZE];
    payload[0..8].copy_from_slice(&seq.to_le_bytes());
    payload[8..16].copy_from_slice(&unix_now_ns_u64().to_le_bytes());
    payload
}

fn spawn_mock_bench_driver(
    period: Duration,
    tx: mpsc::Sender<SessionEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = time::interval(period);
        let mut seq = 0_u64;

        loop {
            ticker.tick().await;
            seq = seq.wrapping_add(1);
            let payload = make_bench_payload(seq).to_vec();

            if tx.send(SessionEvent::BenchMessage(payload)).await.is_err() {
                warn!("session channel closed, stopping mock bench driver");
                break;
            }
        }
    })
}

fn spawn_udp_receiver(
    bind_addr: SocketAddr,
    session_tx: mpsc::Sender<SessionEvent>,
) -> tokio::task::JoinHandle<Result<(), DynError>> {
    tokio::spawn(async move {
        let socket = UdpSocket::bind(bind_addr).await?;
        info!(%bind_addr, "bench udp receiver bound");

        let mut buf = [0_u8; 1024];
        loop {
            let (len, peer) = socket.recv_from(&mut buf).await?;
            debug!(%peer, bytes = len, "bench udp packet received");
            if session_tx
                .send(SessionEvent::ClientPacket { peer })
                .await
                .is_err()
            {
                warn!("session channel closed, stopping udp receiver");
                break;
            }
        }
        Ok(())
    })
}

fn spawn_session_state_machine(
    mut session_rx: mpsc::Receiver<SessionEvent>,
    udp_tx: mpsc::Sender<UdpSendRequest>,
    client_timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut state = ServerState::WaitingForClient;
        let mut timeout_tick = time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                _ = timeout_tick.tick() => {
                    if let ServerState::ConnectedToClient { client_addr, last_seen } = state
                        && last_seen.elapsed() >= client_timeout
                    {
                        info!(
                            %client_addr,
                            timeout_ms = client_timeout.as_millis(),
                            "state transition: ConnectedToClient -> WaitingForClient (timeout)"
                        );
                        state = ServerState::WaitingForClient;
                    }
                }
                maybe_event = session_rx.recv() => {
                    let Some(event) = maybe_event else { break; };
                    match event {
                        SessionEvent::ClientPacket { peer } => match state {
                            ServerState::WaitingForClient => {
                                state = ServerState::ConnectedToClient {
                                    client_addr: peer,
                                    last_seen: Instant::now(),
                                };
                                info!(%peer, "state transition: WaitingForClient -> ConnectedToClient");
                            }
                            ServerState::ConnectedToClient { client_addr, .. } if client_addr != peer => {
                                warn!(
                                    %peer,
                                    expected = %client_addr,
                                    "ignoring packet from non-active client"
                                );
                            }
                            ServerState::ConnectedToClient { client_addr, .. } => {
                                state = ServerState::ConnectedToClient {
                                    client_addr,
                                    last_seen: Instant::now(),
                                };
                            }
                        },
                        SessionEvent::BenchMessage(payload) => match state {
                            ServerState::WaitingForClient => {
                                debug!("dropping bench message while waiting for client");
                            }
                            ServerState::ConnectedToClient { client_addr, .. } => {
                                if udp_tx
                                    .send(UdpSendRequest {
                                        peer: client_addr,
                                        payload,
                                    })
                                    .await
                                    .is_err()
                                {
                                    warn!("udp send channel closed, stopping session");
                                    break;
                                }
                            }
                        },
                    }
                }
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
        info!(%bind_addr, "bench udp sender ready");

        while let Some(req) = rx.recv().await {
            let sent = socket.send_to(&req.payload, req.peer).await?;
            debug!(sent, peer = %req.peer, "bench datagram sent");
        }

        warn!("udp sender channel closed");
        Ok(())
    })
}

fn get_arg(args: &[String], key: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == key).map(|w| w[1].clone())
}

fn parse_socket_arg(args: &[String], key: &str, default: &str) -> Result<SocketAddr, DynError> {
    let value = get_arg(args, key).unwrap_or_else(|| default.to_owned());
    Ok(value.parse()?)
}

fn parse_u64_arg(args: &[String], key: &str, default: u64) -> Result<u64, DynError> {
    match get_arg(args, key) {
        Some(v) => Ok(v.parse()?),
        None => Ok(default),
    }
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

    let args: Vec<String> = std::env::args().collect();
    let recv_addr = parse_socket_arg(&args, "--recv-addr", "127.0.0.1:7001")?;
    let send_bind_addr = parse_socket_arg(&args, "--send-bind-addr", "127.0.0.1:0")?;
    let period_ms = parse_u64_arg(&args, "--period-ms", 20)?;
    let client_timeout_ms = parse_u64_arg(&args, "--client-timeout-ms", 2000)?;
    let period = Duration::from_millis(period_ms);
    let client_timeout = Duration::from_millis(client_timeout_ms);

    let (session_tx, session_rx) = mpsc::channel::<SessionEvent>(1024);
    let (udp_tx, udp_rx) = mpsc::channel::<UdpSendRequest>(1024);

    let _driver_task = spawn_mock_bench_driver(period, session_tx.clone());
    let _session_task = spawn_session_state_machine(session_rx, udp_tx, client_timeout);
    let recv_task = spawn_udp_receiver(recv_addr, session_tx);
    let _send_task = spawn_udp_sender(send_bind_addr, udp_rx);

    info!(
        %recv_addr,
        %send_bind_addr,
        period_ms,
        client_timeout_ms,
        msg_size = BENCH_MESSAGE_SIZE,
        "bench server started"
    );
    info!("first UDP packet claims active client");

    recv_task.await??;
    Ok(())
}
