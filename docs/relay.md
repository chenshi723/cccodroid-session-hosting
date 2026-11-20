# 自行部署中转

依赖 Python 3.11+ 与 `relay/requirements.txt`。建议以独立、无特权的系统用户运行单一中转实例，使用仅该账户可访问的本地状态目录。不要共享状态文件给多个进程。

## 监听回环

安装依赖后，在可信终端中通过密码管理器准备一个 **32 字节安全随机值编码为 base64url（43 字符，无填充）** 的注册令牌。它只用于建立房间，不是模型 API Key、配对码或房间凭据。若在本机用 Python 的 `secrets.token_urlsafe(32)` 生成，请不要把结果留在共享终端、会话记录或命令历史里。

以下命令适用于 Bash。令牌通过无回显输入进入进程环境，不作为命令参数；不要开启 shell 跟踪、导出诊断环境或共享该服务账户。

```sh
umask 077
state_dir="$HOME/.local/state/cccodroid-session-relay"
mkdir -p -m 700 "$state_dir"
read -rsp 'Relay enrollment token: ' enroll
printf '\n'
CC_HOST_ENROLL_TOKEN="$enroll" \
CC_HOST_STATE="$state_dir/rooms.json" \
CC_HOST_BIND=127.0.0.1 \
CC_HOST_PORT=7865 \
python -B relay/server.py
unset enroll
```

在同一机器的另一个终端检查健康状态（无需凭据）：

```sh
curl --fail --silent --show-error http://127.0.0.1:7865/cc-host/v1/health
```

预期响应为 `{"ok":true,"protocol":1}`（空格可能不同）。回环 HTTP 仅用于本机健康检查与测试，不是生产设备的配对地址。运行前确认状态目录权限确实只允许服务账户访问；启动时若发现状态损坏，应停机检查，不要删掉旧文件假装恢复。

## 生产设备接入

- 在自己控制的 HTTPS 反向代理上仅公开 `/cc-host/v1/`，保留该路径前缀，关闭缓存与请求/响应正文日志；不要转发到 Android 网关。设备填写自己的 HTTPS origin，例如 `https://relay.example.invalid` 是不可连接的占位符。
- 禁用或脱敏反代、网关、边缘服务的访问与错误日志，不记录 `Authorization`、请求体、配对码或房间路径。TLS 终点可见传输凭据和请求元数据，不要用不信任的终止代理。
- 允许协议的长轮询与返回时限（主机轮询最多 20 秒，控制端等待主机最多 25 秒）；上游超时要留出余量。保留请求体限制并对外部连接施加合理上限；不要用自动 POST 重试掩盖回执不明。
- 服务端状态文件保存房间标识、传输凭据散列与有界吊销回执。待处理的包只在内存中，不会在重启后续送。状态文件和备份仍属私密；不要恢复会重新启用已撤销房间的旧备份。生产密钥注入与进程守护由部署者配置，不附带任何现成服务配置。

默认不开放非回环监听。若只想检查功能，仓库测试会建立临时状态并自行启动/停止回环实例，不需要先安装一个常驻的中转服务。
