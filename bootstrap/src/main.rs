use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use libp2p::Multiaddr;
use tracing::info;

/// SwarmDrop 引导+中继节点
#[derive(Parser, Debug)]
#[command(name = "swarm-bootstrap")]
#[command(version, about = "SwarmDrop DHT bootstrap and relay server")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// 启动引导+中继节点
    Run {
        /// TCP 监听端口
        #[arg(long, default_value = "4001")]
        tcp_port: u16,

        /// QUIC 监听端口
        #[arg(long, default_value = "4001")]
        quic_port: u16,

        /// 密钥文件路径（默认从二进制所在目录查找 identity.key）
        #[arg(long)]
        key_file: Option<PathBuf>,

        /// 监听的 IP 地址
        #[arg(long, default_value = "0.0.0.0")]
        listen_addr: String,

        /// 空闲连接超时（秒）
        #[arg(long, default_value = "120")]
        idle_timeout: u64,

        /// 公网 IP 地址（relay server 必须设置，否则 reservation 响应不含地址）
        #[arg(long)]
        external_ip: Option<String>,
    },

    /// 打印节点 PeerId 后退出
    PeerId {
        /// 密钥文件路径（默认从二进制所在目录查找 identity.key）
        #[arg(long)]
        key_file: Option<PathBuf>,
    },
}

/// 按优先级解析密钥文件路径：
/// 1. 用户显式传入
/// 2. 二进制所在目录的 identity.key
/// 3. 当前目录的 identity.key（兜底）
fn resolve_key_file(key_file: Option<PathBuf>) -> PathBuf {
    key_file.unwrap_or_else(|| {
        std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|p| p.join("identity.key")))
            .unwrap_or_else(|| PathBuf::from("identity.key"))
    })
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::PeerId { key_file } => {
            let key_file = resolve_key_file(key_file);
            let keypair = swarm_bootstrap::util::load_or_generate_keypair(&key_file)?;
            println!("{}", keypair.public().to_peer_id());
        }

        Command::Run {
            tcp_port,
            quic_port,
            key_file,
            listen_addr,
            idle_timeout,
            external_ip,
        } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info".into()),
                )
                .init();

            let key_file = resolve_key_file(key_file);
            let keypair = swarm_bootstrap::util::load_or_generate_keypair(&key_file)?;
            let peer_id = keypair.public().to_peer_id();
            info!("Node PeerId: {}", peer_id);

            let tcp_addr: Multiaddr = format!("/ip4/{}/tcp/{}", listen_addr, tcp_port).parse()?;
            let quic_addr: Multiaddr =
                format!("/ip4/{}/udp/{}/quic-v1", listen_addr, quic_port).parse()?;

            info!("TCP listen address: {}", tcp_addr);
            info!("QUIC listen address: {}", quic_addr);

            let external_addrs: Vec<Multiaddr> = if let Some(ref ip) = external_ip {
                vec![
                    format!("/ip4/{}/tcp/{}", ip, tcp_port).parse()?,
                    format!("/ip4/{}/udp/{}/quic-v1", ip, quic_port).parse()?,
                ]
            } else {
                vec![]
            };

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(swarm_bootstrap::run(
                    keypair,
                    tcp_addr,
                    quic_addr,
                    Duration::from_secs(idle_timeout),
                    external_addrs,
                ))?;
        }
    }

    Ok(())
}
