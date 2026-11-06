# cpa-plugin-codex-overload-retry

[CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) 原生插件：自动吸收 Codex 上游的 **"Selected model is at capacity"** 过载错误，在客户端无感知的情况下退避重试，直到成功。

如果你在用 CPA 中转 Codex（OAuth 账号或 API 中转渠道），一定见过这个错误：

```json
{"error":{"type":"service_unavailable_error","code":"server_is_overloaded",
"message":"Our servers are currently overloaded. Please try again later."}}
```

这个错误以 **HTTP 200 + SSE 带内错误**（或 502）的形式返回，CPA 的常规重试策略对它无效，错误会直接砸到 Codex CLI，打断你的会话。本插件在流式响应的头部被送达客户端之前进行拦截与分类，过载错误就地吞掉、退避、换凭据重发；真实输出一旦出现立即透传。

## 工作原理

```text
Codex CLI ──▶ CPA ──▶ [ModelRouter] 匹配 codex 请求，路由到本插件执行器
                          │
                          ▼
                    [Executor] 嵌套调用 host.model.execute_stream
                    （携带 host_callback_id，宿主跳过本插件，正常走凭据选择）
                          │
                ┌─────────┴──────────┐
                ▼                    ▼
        流头缓冲 + SSE 分类      检测到过载错误
        response.created 等 → 缓冲   server_is_overloaded / at capacity
        真实输出 → 提交转发          → 关流 → 退避 → 重试（换凭据）
```

关键设计：

- **commit 前重试**：只有还没向客户端发出任何字节时才重试，保证请求不会被重复消费；一旦出现真实输出，立即进入透传模式，后续字节原样转发
- **凭据轮换**：每次重试都经过宿主完整的凭据选择链路（fill-first / round-robin、会话亲和、cooldown），多个 Codex 账号自动轮换
- **遵从上游 retry-after**：解析错误体里的 `retry-after-ms`（Codex 上游会给出约 300 秒的恢复预估），硬故障时长时间等待而不是高频硬打
- **精确分类**：quota / billing / 鉴权类错误不重试，直接透传；只吸收真正的容量类错误
- **递归保护**：嵌套执行带 `host_callback_id`，宿主自动跳过本插件的路由与拦截器

## 安装

### 方式一：自行构建（推荐）

```bash
git clone https://github.com/D4wnnn/cpa-plugin-codex-overload-retry.git
cd cpa-plugin-codex-overload-retry
bash build.sh              # 产出 cpa-plugin-codex-overload-retry.so
bash build.sh --deploy     # 构建并复制到 CPA 插件目录
```

需要 Go ≥ 1.21 与 CGO（gcc）。

### 方式二：手动放置

把 `.so` 复制到 CPA 的插件目录（相对 CPA 工作目录）：

```text
plugins/linux/amd64/cpa-plugin-codex-overload-retry.so
```

### 配置

在 `config.yaml` 中启用：

```yaml
plugins:
  enabled: true
  configs:
    cpa-plugin-codex-overload-retry:
      enabled: true
      priority: 10
```

重启 CPA 生效。日志出现以下行即加载成功：

```text
pluginhost: plugin loaded plugin_id=cpa-plugin-codex-overload-retry
pluginhost: plugin registered plugin_id=cpa-plugin-codex-overload-retry plugin_name=cpa-plugin-codex-overload-retry version=0.1.0
```

## 配置参考

所有字段均可省略，默认值如下：

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `enabled` | `true` | 总开关，`false` 时所有请求回落到内置链路 |
| `source_formats` | `["openai-response", "openai"]` | 参与包装的客户端协议（Responses API / Chat Completions） |
| `require_codex_provider` | `true` | 只处理模型注册在 codex 凭据下的请求（通过 `host.auth.models` 校验） |
| `max_attempts` | `30` | 单个请求最大上游尝试次数 |
| `total_budget_seconds` | `900` | 单个请求总重试预算（秒） |
| `commit_deadline_seconds` | `120` | 流头判定的最长缓冲时间（秒），超时强制提交 |
| `backoff_base_seconds` | `1.0` | 首次重试延迟 |
| `backoff_factor` | `1.6` | 指数退避倍率 |
| `backoff_max_seconds` | `8` | 指数退避上限 |
| `max_retry_wait_seconds` | `300` | 遵从上游 retry-after 时的单次等待上限 |
| `prelude_limit_bytes` | `262144` | 流头缓冲上限（字节），超过即提交 |
| `retry_http_status` | `[429, 500, 502, 503, 504]` | 触发重试的 HTTP 状态码（非 quota/鉴权错误时） |
| `log_retries` | `true` | 每次吸收的重试记录到 CPA 日志 |
| `log_routes` | `true` | 每个被包装的请求记录到 CPA 日志 |

## 状态页

插件内置一个监控页面（每 10 秒自动刷新），显示：包装请求数、吸收的重试数、重试耗尽数、按模型统计、最近 30 条重试事件及原因：

```text
http://<cpa-host>:<port>/v0/resource/plugins/cpa-plugin-codex-overload-retry/status
```

同时出现在管理面板（Management Center）的插件菜单中。页面不含任何密钥或凭证信息。

## 已知限制

- 重试期间客户端断开连接无法立即感知，重试会持续到预算耗尽（默认 15 分钟上限内）
- 统计数据保存在插件进程内存中，CPA 重启后清零
- 多个 Codex 凭据同时进入 cooldown 时会出现 `auth_unavailable`，插件会将其视为可重试错误继续等待

## 致谢

- **[NewBeeM4n/codex-overload-fix](https://github.com/NewBeeM4n/codex-overload-fix)**（MIT）—— 本插件的过载分类与重试策略移植自该项目的 `relay.py`，感谢作者的实测沉淀（`server_is_overloaded` 等错误码集合、SSE 多行帧解析、commit 前重试语义）
- **[router-for-me/CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI)** —— 插件宿主，提供了干净的原生插件 ABI（ModelRouter / Executor / host callbacks）

## License

[MIT](./LICENSE)
