# cccodroid 会话托管 / Session Hosting

让已配对的设备查看和操作主机**主动分享的会话**。会话仍在主机运行；控制端可以同步对话、发送文字、停止本轮、回答会话内的授权请求。文件、终端、模型配置和系统级确认仍由主机管理。

This is the self-hosted relay, host/controller engine, and cccodroid integration source for opt-in remote session hosting. It is not a full app, a hosted service, or a plug-and-play patch for stock cccodroid.

## 这个仓库包含什么

| 路径 | 内容 |
| --- | --- |
| `relay/` | 可独立部署的 Python 中转及测试。中转只转发加密信封，不执行会话。 |
| `gateway/gw/hosting/core.mjs`、`engine.mjs` | 主机和控制端共用的配对、加密、会话授权、同步和撤销引擎。Node 内置模块即可运行。 |
| `gateway/gw/hosting/adapter.mjs`、`index.mjs`、`gateway/web/hosting/` | 兼容版 cccodroid 的网关接入及托管面板源码；依赖宿主的会话、运行、回收和聊天界面接口。 |
| `tests/` | 合成会话测试，包括真实中转与两个引擎的回环联测。不会启动模型、不读设备会话。 |

中转可独立运行；Android 端需要**已集成会话托管的 cccodroid 兼容版**。本仓库不含整套 fork、APK、设备配置或现成的公共中转地址，不能直接覆盖安装。移植约束见[集成说明](docs/integration.md)。

## 使用流程

1. 按[部署说明](docs/relay.md)启动自己的中转，生产设备使用有效证书的 HTTPS 地址。
2. 在兼容版应用的「会话托管」中设定主机，填写你自己的中转地址和注册令牌。选择要托管的**现有会话**；没有默认分享。
3. 仅把主机生成的配对码通过可信渠道交给自己的控制端。控制端导入后，从主机列表选择会话。
4. 不再需要时，在主机取消托管。若配对码外泄，停用并解除整个房间，确认中转吊销成功后再重新配对；只在控制端「忘记设备」不会撤销旧配对码。

配对码不是一次性验证码，持有者可访问该房间当前及以后分享的会话。分享前确认会话内容和主机权限设置；已经同步出去的文字无法撤回。中转可见请求元数据，详细边界见[安全说明](SECURITY.md)。需要主机保持联网和运行；系统结束进程、设备断网或关机后不会继续远程工作。

## 本地验证

使用支持 POSIX 文件权限的环境、Python 3.11+、Node.js 22+。先安装中转依赖，再跑测试：

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r relay/requirements.txt
python -B -m unittest discover -s relay -p 'test_relay.py'
HOSTING_TEST_PYTHON="$PWD/.venv/bin/python" node --test tests/*.test.mjs
```

测试不需要上线服务器、API Key、Android 应用或已有配对码。会话和凭据都是临时测试数据；真实中转联测仅监听回环地址。它不等同于 Android WebView 或双机实测。

## 许可

[Apache License 2.0](LICENSE)。基于 [cccodroid](https://github.com/greyoak111/cccodroid) 的接入，归属详见 [NOTICE](NOTICE)。本项目不是 Anthropic 官方产品，不分发 Claude Code。
