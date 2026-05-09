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
    https://raw.githubusercontent.com/swarm-apps/swarm-p2p/main/bootstrap/swarm-bootstrap.service
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

swarm-bootstrap peer-id [OPTIONS]
    --key-file <PATH>       密钥文件路径           [默认: 二进制所在目录/identity.key]
```

`run` 的日志级别通过 `RUST_LOG` 环境变量控制，默认 `info`。

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
