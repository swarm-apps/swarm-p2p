# swarm-bootstrap

SwarmDrop 的 DHT 引导 + Relay 中继节点，部署在公网 VPS 上为客户端提供：

- **Kademlia DHT Server** — 响应路由查询，帮助客户端互相发现
- **Relay Server** — 为 NAT 后的节点中继流量，配合 DCUtR 打洞
- **AutoNAT v2 Server** — 响应客户端的 NAT 检测请求（回拨探测）

## 服务器要求

预构建二进制为 `x86_64-unknown-linux-musl` 静态编译：

- **操作系统：** Linux
- **架构：** x86_64（AMD64）

如果你的服务器是其他架构（如 ARM64），请参考[从源码构建](#从源码构建)自行编译。

## 部署

### 1. 下载二进制

从 [GitHub Releases](https://github.com/swarm-apps/swarm-p2p/releases?q=bootstrap-v) 下载最新版本的 `swarm-bootstrap`（musl 静态编译，无依赖）：

```bash
# 下载并赋予执行权限
wget https://github.com/swarm-apps/swarm-p2p/releases/latest/download/swarm-bootstrap
chmod +x swarm-bootstrap
```

### 2. 安装二进制

```bash
sudo mkdir -p /opt/swarm-bootstrap
sudo mv swarm-bootstrap /opt/swarm-bootstrap/
sudo ln -s /opt/swarm-bootstrap/swarm-bootstrap /usr/local/bin/swarm-bootstrap
```

### 3. 配置 systemd 服务

下载服务文件：

```bash
sudo wget -O /etc/systemd/system/swarm-bootstrap.service \
    https://raw.githubusercontent.com/yexiyue/swarm-p2p/main/bootstrap/swarm-bootstrap.service
```

**编辑服务文件，添加公网 IP**（Relay 必须设置，否则客户端无法通过本节点中继）：

```bash
sudo systemctl edit swarm-bootstrap
```

在编辑器中添加：

```ini
[Service]
ExecStart=
ExecStart=/opt/swarm-bootstrap/swarm-bootstrap run \
    --tcp-port 4001 \
    --quic-port 4001 \
    --external-ip <你的公网IP>
```

### 4. 启动服务

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now swarm-bootstrap

# 查看日志
journalctl -u swarm-bootstrap -f
```

启动后可通过以下命令查看节点 PeerId：

```bash
swarm-bootstrap peer-id
# 12D3KooW...
```

将 PeerId 与公网 IP 拼成完整 multiaddr，配置到客户端：

```
/ip4/<公网IP>/tcp/4001/p2p/12D3KooW...
/ip4/<公网IP>/udp/4001/quic-v1/p2p/12D3KooW...
```

### 5. 开放防火墙

```bash
sudo ufw allow 4001/tcp
sudo ufw allow 4001/udp
```

## Docker / Coolify 部署

`swarm-bootstrap` 也提供容器镜像，适合在 Coolify 等面板中一键部署：

```text
ghcr.io/swarm-apps/swarm-bootstrap:latest
ghcr.io/swarm-apps/swarm-bootstrap:0.4.1
```

镜像发布为多架构 manifest，支持 `linux/amd64` 和 `linux/arm64`。Docker 会根据服务器架构自动选择对应镜像。

### Coolify

在 Coolify 中创建 Docker Compose 资源，使用仓库里的 [`compose.coolify.yml`](compose.coolify.yml)。至少需要设置：

```env
SWARM_BOOTSTRAP_EXTERNAL_IP=<你的公网IP>
```

部署后需要确认云服务器安全组和系统防火墙开放：

```bash
4001/tcp
4001/udp
```

### Docker Compose

```yaml
services:
  swarm-bootstrap:
    image: ghcr.io/swarm-apps/swarm-bootstrap:latest
    restart: unless-stopped
    ports:
      - "4001:4001/tcp"
      - "4001:4001/udp"
    volumes:
      - swarm-bootstrap-data:/data
    environment:
      SWARM_BOOTSTRAP_EXTERNAL_IP: "<你的公网IP>"
      SWARM_BOOTSTRAP_KEY_FILE: /data/identity.key
      RUST_LOG: info

volumes:
  swarm-bootstrap-data:
```

查看 PeerId：

```bash
docker run --rm \
  -v swarm-bootstrap-data:/data \
  ghcr.io/swarm-apps/swarm-bootstrap:latest peer-id
```

> `identity.key` 决定 PeerId。容器部署时务必持久化 `/data/identity.key`，不要随意删除 volume。

## CLI

```
Commands:
  run       启动引导+中继节点
  peer-id   打印节点 PeerId 后退出

swarm-bootstrap run [OPTIONS]
    --tcp-port <PORT>       TCP 监听端口          [默认: 4001]
    --quic-port <PORT>      QUIC 监听端口         [默认: 4001]
    --key-file <PATH>       密钥文件路径           [默认: 二进制所在目录/identity.key]
    --listen-addr <IP>      监听 IP 地址           [默认: 0.0.0.0]
    --idle-timeout <SECS>   空闲连接超时(秒)       [默认: 120]
    --external-ip <IP>      公网 IP 地址（Relay Server 必须设置）
    --max-reservations <N>                最大活跃 reservation 数       [默认: 128]
    --max-reservations-per-peer <N>       单 peer 最大 reservation 数    [默认: 4]
    --reservation-duration-secs <SECS>    reservation 有效期(秒)         [默认: 3600]
    --max-circuits <N>                    最大活跃 circuit 数           [默认: 16]
    --max-circuits-per-peer <N>           单 peer 最大 circuit 数        [默认: 4]
    --max-circuit-duration-secs <SECS>    单 circuit 最长持续时间(秒)    [默认: 3600]
    --max-circuit-bytes <BYTES>           单 circuit 最大转发字节数      [默认: 536870912]

swarm-bootstrap peer-id [OPTIONS]
    --key-file <PATH>       密钥文件路径           [默认: 二进制所在目录/identity.key]
```

`run` 的日志级别通过 `RUST_LOG` 环境变量控制，默认 `info`。

常用环境变量：

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `SWARM_BOOTSTRAP_EXTERNAL_IP` | 公网 IP，Relay 必须设置 | 无 |
| `SWARM_BOOTSTRAP_TCP_PORT` | TCP 监听端口 | `4001` |
| `SWARM_BOOTSTRAP_QUIC_PORT` | QUIC 监听端口 | `4001` |
| `SWARM_BOOTSTRAP_KEY_FILE` | 密钥文件路径 | 二进制目录下的 `identity.key` |
| `SWARM_BOOTSTRAP_LISTEN_ADDR` | 监听 IP | `0.0.0.0` |
| `SWARM_BOOTSTRAP_IDLE_TIMEOUT_SECS` | 空闲连接超时 | `120` |
| `SWARM_BOOTSTRAP_MAX_RESERVATIONS` | 最大活跃 reservation 数 | `128` |
| `SWARM_BOOTSTRAP_MAX_RESERVATIONS_PER_PEER` | 单 peer 最大 reservation 数 | `4` |
| `SWARM_BOOTSTRAP_RESERVATION_DURATION_SECS` | reservation 有效期 | `3600` |
| `SWARM_BOOTSTRAP_MAX_CIRCUITS` | 最大活跃 circuit 数 | `16` |
| `SWARM_BOOTSTRAP_MAX_CIRCUITS_PER_PEER` | 单 peer 最大 circuit 数 | `4` |
| `SWARM_BOOTSTRAP_MAX_CIRCUIT_DURATION_SECS` | 单 circuit 最长持续时间 | `3600` |
| `SWARM_BOOTSTRAP_MAX_CIRCUIT_BYTES` | 单 circuit 最大转发字节数，`0` 表示不限制 | `536870912` |

## 密钥管理

- 首次启动自动生成 Ed25519 密钥对，保存为 `identity.key`
- 密钥决定 PeerId，**丢失密钥 = PeerId 改变 = 所有客户端需更新配置**
- 请妥善备份 `/opt/swarm-bootstrap/identity.key`

## 从源码构建

```bash
cargo build --release -p swarm-bootstrap
```

## 协议栈

| 协议 | 作用 |
|------|------|
| Ping | 心跳保活（间隔 15s，超时 10s） |
| Identify | 节点信息交换，`protocol_version` 为 `/swarmdrop/1.0.0`，必须与客户端一致 |
| Kademlia | DHT Server 模式，record TTL 2h，replication factor 20 |
| Relay | 中继服务端，circuit 上限 512MB / 1h |
| AutoNAT v2 | Server 端，帮助客户端判断自身 NAT 状态 |

## 设计文档

详见 [docs/bootstrap-relay-node.md](../docs/bootstrap-relay-node.md)。
