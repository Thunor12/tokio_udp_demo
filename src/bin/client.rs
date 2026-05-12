use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

type DynError = Box<dyn Error + Send + Sync + 'static>;

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

    let local_bind: SocketAddr = "127.0.0.1:7002".parse()?;
    let server_addr: SocketAddr = "127.0.0.1:7001".parse()?;

    let socket = Arc::new(UdpSocket::bind(local_bind).await?);
    info!(%local_bind, %server_addr, "udp client started");
    info!("type a message and press enter to send");

    let recv_socket = Arc::clone(&socket);
    let recv_task = tokio::spawn(async move {
        let mut buf = vec![0_u8; 2048];
        loop {
            let (len, peer) = recv_socket.recv_from(&mut buf).await?;
            let payload = &buf[..len];
            info!(
                %peer,
                bytes = len,
                payload = String::from_utf8_lossy(payload).as_ref(),
                "received udp packet"
            );
        }
        #[allow(unreachable_code)]
        Ok::<(), DynError>(())
    });

    let stdin = io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        if line.trim() == "/quit" {
            info!("quitting client");
            break;
        }

        let bytes = line.into_bytes();
        let sent = socket.send_to(&bytes, server_addr).await?;
        debug!(sent, %server_addr, "sent udp packet");
    }

    recv_task.abort();
    match recv_task.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => warn!(%err, "receiver task ended with error"),
        Err(join_err) if join_err.is_cancelled() => {}
        Err(join_err) => warn!(%join_err, "receiver task join error"),
    }

    Ok(())
}
