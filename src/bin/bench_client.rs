use std::error::Error;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::UdpSocket;
use tokio::time::{self, Duration, Instant};
use tracing::{debug, info, warn};

type DynError = Box<dyn Error + Send + Sync + 'static>;

const BENCH_MESSAGE_SIZE: usize = 56;

#[derive(Debug, Default)]
struct BenchStats {
    received: u64,
    invalid_size: u64,
    clock_skew_count: u64,
    out_of_order: u64,
    dropped_estimate: u64,
    last_seq: Option<u64>,
    latencies_ns: Vec<u64>,
}

#[derive(Debug)]
struct BenchReport {
    phase: &'static str,
    received: u64,
    invalid_size: u64,
    out_of_order: u64,
    dropped_estimate: u64,
    clock_skew_count: u64,
    min_us: f64,
    avg_us: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    max_us: f64,
}

impl BenchStats {
    fn on_packet(&mut self, packet: &[u8]) {
        if packet.len() != BENCH_MESSAGE_SIZE {
            self.invalid_size = self.invalid_size.saturating_add(1);
            return;
        }

        let seq = u64::from_le_bytes(packet[0..8].try_into().expect("slice size checked"));
        let sent_ns = u64::from_le_bytes(packet[8..16].try_into().expect("slice size checked"));
        let now_ns = unix_now_ns_u64();

        if let Some(last) = self.last_seq {
            if seq <= last {
                self.out_of_order = self.out_of_order.saturating_add(1);
            } else {
                self.dropped_estimate = self
                    .dropped_estimate
                    .saturating_add(seq.saturating_sub(last + 1));
            }
        }
        self.last_seq = Some(seq);

        if now_ns < sent_ns {
            self.clock_skew_count = self.clock_skew_count.saturating_add(1);
            return;
        }

        self.received = self.received.saturating_add(1);
        self.latencies_ns.push(now_ns - sent_ns);
    }

    fn print_report(&self, label: &str) {
        if self.latencies_ns.is_empty() {
            info!(
                phase = label,
                received = self.received,
                invalid_size = self.invalid_size,
                out_of_order = self.out_of_order,
                dropped_estimate = self.dropped_estimate,
                clock_skew_count = self.clock_skew_count,
                "no valid latency samples yet"
            );
            return;
        }

        let mut sorted = self.latencies_ns.clone();
        sorted.sort_unstable();

        let min = sorted[0];
        let max = *sorted.last().unwrap_or(&min);
        let avg = sorted.iter().map(|v| *v as u128).sum::<u128>() / sorted.len() as u128;
        let p50 = percentile(&sorted, 50.0);
        let p95 = percentile(&sorted, 95.0);
        let p99 = percentile(&sorted, 99.0);

        info!(
            phase = label,
            received = self.received,
            invalid_size = self.invalid_size,
            out_of_order = self.out_of_order,
            dropped_estimate = self.dropped_estimate,
            clock_skew_count = self.clock_skew_count,
            min_us = ns_to_us(min),
            avg_us = ns_to_us(avg as u64),
            p50_us = ns_to_us(p50),
            p95_us = ns_to_us(p95),
            p99_us = ns_to_us(p99),
            max_us = ns_to_us(max),
            "latency stats"
        );
    }

    fn to_report(&self, phase: &'static str) -> BenchReport {
        if self.latencies_ns.is_empty() {
            return BenchReport {
                phase,
                received: self.received,
                invalid_size: self.invalid_size,
                out_of_order: self.out_of_order,
                dropped_estimate: self.dropped_estimate,
                clock_skew_count: self.clock_skew_count,
                min_us: 0.0,
                avg_us: 0.0,
                p50_us: 0.0,
                p95_us: 0.0,
                p99_us: 0.0,
                max_us: 0.0,
            };
        }

        let mut sorted = self.latencies_ns.clone();
        sorted.sort_unstable();
        let min = sorted[0];
        let max = *sorted.last().unwrap_or(&min);
        let avg = sorted.iter().map(|v| *v as u128).sum::<u128>() / sorted.len() as u128;
        let p50 = percentile(&sorted, 50.0);
        let p95 = percentile(&sorted, 95.0);
        let p99 = percentile(&sorted, 99.0);

        BenchReport {
            phase,
            received: self.received,
            invalid_size: self.invalid_size,
            out_of_order: self.out_of_order,
            dropped_estimate: self.dropped_estimate,
            clock_skew_count: self.clock_skew_count,
            min_us: ns_to_us(min),
            avg_us: ns_to_us(avg as u64),
            p50_us: ns_to_us(p50),
            p95_us: ns_to_us(p95),
            p99_us: ns_to_us(p99),
            max_us: ns_to_us(max),
        }
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * ((sorted.len() - 1) as f64)).round() as usize;
    sorted[idx]
}

fn ns_to_us(ns: u64) -> f64 {
    ns as f64 / 1_000.0
}

fn unix_now_ns_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn make_uplink_payload(seq: u64) -> [u8; BENCH_MESSAGE_SIZE] {
    let mut payload = [0_u8; BENCH_MESSAGE_SIZE];
    payload[0..8].copy_from_slice(&seq.to_le_bytes());
    payload[8..16].copy_from_slice(&unix_now_ns_u64().to_le_bytes());
    payload
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

fn write_csv_report(path: &str, report: &BenchReport, sent_to_server_total: u64) -> Result<(), DynError> {
    let content = format!(
        "phase,received,invalid_size,out_of_order,dropped_estimate,clock_skew_count,sent_to_server_total,min_us,avg_us,p50_us,p95_us,p99_us,max_us\n{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}\n",
        report.phase,
        report.received,
        report.invalid_size,
        report.out_of_order,
        report.dropped_estimate,
        report.clock_skew_count,
        sent_to_server_total,
        report.min_us,
        report.avg_us,
        report.p50_us,
        report.p95_us,
        report.p99_us,
        report.max_us
    );
    std::fs::write(path, content)?;
    Ok(())
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
    let local_bind = parse_socket_arg(&args, "--bind-addr", "127.0.0.1:7002")?;
    let server_addr = parse_socket_arg(&args, "--server-addr", "127.0.0.1:7001")?;
    let duration_sec = parse_u64_arg(&args, "--duration-sec", 10)?;
    let report_ms = parse_u64_arg(&args, "--report-ms", 1000)?;
    let send_period_ms = parse_u64_arg(&args, "--send-period-ms", 20)?;
    let csv_out = get_arg(&args, "--csv-out");

    let socket = UdpSocket::bind(local_bind).await?;
    info!(
        %local_bind,
        %server_addr,
        duration_sec,
        report_ms,
        send_period_ms,
        "bench client started"
    );

    let handshake = make_uplink_payload(0);
    let sent = socket.send_to(&handshake, server_addr).await?;
    debug!(sent, %server_addr, "sent handshake packet");

    let deadline = Instant::now() + Duration::from_secs(duration_sec);
    let mut ticker = time::interval(Duration::from_millis(report_ms));
    let mut send_ticker = time::interval(Duration::from_millis(send_period_ms));
    let mut buf = [0_u8; 2048];
    let mut cumulative = BenchStats::default();
    let mut window = BenchStats::default();
    let mut active_server_peer: Option<SocketAddr> = None;
    let mut uplink_seq: u64 = 0;
    let mut sent_to_server_total: u64 = 1;
    let mut sent_to_server_window: u64 = 1;

    loop {
        if Instant::now() >= deadline {
            break;
        }

        tokio::select! {
            _ = ticker.tick() => {
                window.print_report("window");
                cumulative.print_report("cumulative");
                info!(
                    phase = "window",
                    sent_to_server = sent_to_server_window,
                    sent_to_server_cumulative = sent_to_server_total,
                    "uplink send stats"
                );
                window = BenchStats::default();
                sent_to_server_window = 0;
            }
            _ = send_ticker.tick() => {
                uplink_seq = uplink_seq.wrapping_add(1);
                let payload = make_uplink_payload(uplink_seq);
                let sent = socket.send_to(&payload, server_addr).await?;
                debug!(sent, %server_addr, uplink_seq, "sent uplink packet");
                sent_to_server_total = sent_to_server_total.saturating_add(1);
                sent_to_server_window = sent_to_server_window.saturating_add(1);
            }
            recv = socket.recv_from(&mut buf) => {
                let (len, peer) = recv?;
                match active_server_peer {
                    None => {
                        active_server_peer = Some(peer);
                        info!(%peer, "locked onto server sender peer");
                    }
                    Some(expected) if expected != peer => {
                        warn!(%peer, expected = %expected, "ignoring packet from unexpected sender");
                        continue;
                    }
                    Some(_) => {}
                }
                cumulative.on_packet(&buf[..len]);
                window.on_packet(&buf[..len]);
            }
        }
    }

    window.print_report("window-final");
    cumulative.print_report("final");
    info!(
        phase = "final",
        sent_to_server = sent_to_server_window,
        sent_to_server_cumulative = sent_to_server_total,
        "uplink send stats"
    );

    if let Some(csv_path) = csv_out.as_deref() {
        let final_report = cumulative.to_report("final");
        write_csv_report(csv_path, &final_report, sent_to_server_total)?;
        info!(path = csv_path, "wrote benchmark CSV report");
    }

    Ok(())
}
