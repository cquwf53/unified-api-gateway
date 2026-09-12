# Unified API Gateway

一个面向自托管场景的可配置 API 网关。客户端先访问网关，网关根据路由选中一个
profile，再按 profile 的 adapter 和改写规则生成上游请求，最后把上游响应转换回
客户端契约。配置以 YAML 保存，激活配置不需要重启进程。

```text
client -> :8780 data plane -> route/profile -> request transforms -> upstream
                                             <- response transforms <---
                         :8781 admin API / Web console
```

当前仓库先提供 HTTP/JSON 和 LLM 协议适配的基础实现，WebSocket/gRPC 端口和更复杂
的消息级 adapter 可以在同一套 profile 模型上扩展。

## 快速开始

需要 Go 1.26 或更新版本（以 `go.mod` 为准）。在仓库根目录执行：

```powershell
go run ./cmd/gateway --config configs/gateway.yaml
```

默认监听：

| 地址 | 用途 |
| --- | --- |
| `:8780` | 客户端数据面 |
| `127.0.0.1:8781` | 管理 API（默认只绑定本机） |
| `:8782` | gRPC-over-HTTP 透明入口（消息级 Protobuf 改写需 typed adapter） |

存在 SQLite active 版本时，启动默认恢复它而非 `--config` 文件（两者不一致会打
印 warning）；需要强制使用文件时加 `--prefer-file`。CLI 热激活收到管理面
任何非 2xx 响应时都会直接报错退出，不再静默写本地库假装成功；只有管理面无法
连接时才会保存到本地库供下次启动。

也可以使用 Makefile 或 Docker：

```powershell
make run
make validate
make health
docker build -t unified-api-gateway .
docker run --rm -p 8780:8780 -p 127.0.0.1:8781:8781 `
  -e ANTHROPIC_API_KEY=$env:ANTHROPIC_API_KEY `
  -e GATEWAY_ADMIN_TOKEN=$env:GATEWAY_ADMIN_TOKEN unified-api-gateway
```

如果当前系统没有 `make`，直接使用上面的 `go run` 命令即可。容器默认以只读配置
启动，`/app/data` 是 SQLite 持久卷；需要替换初始配置时，将自己的配置目录挂载到
`/app/configs`。容器健康检查调用管理面的 `/readyz`，不是只校验镜像内 YAML。

## 第一个请求

示例配置中的 `echo` upstream 指向一个本地回显服务，适合先验证路由和改写链路。
实际使用时，把 `upstreams.anthropic.base_url` 和密钥引用改成授权的上游地址。

通用 JSON 路由：

```powershell
curl.exe http://127.0.0.1:8780/api/v1/users `
  -H "Content-Type: application/json" `
  -d '{"user_id":123,"name":"Alice"}'
```

OpenAI Chat 客户端可以继续使用原来的路径；网关会把请求适配为 Anthropic
Messages：

```powershell
curl.exe http://127.0.0.1:8780/v1/chat/completions `
  -H "Authorization: Bearer local-client-key" `
  -H "Content-Type: application/json" `
  -d '{"model":"claude-3-5-sonnet","messages":[{"role":"user","content":"hello"}],"max_tokens":64}'
```

客户端密钥由网关验证；上游密钥只从环境变量或受保护的 secret 引用读取。带 SQLite
版本历史启动或热激活时，网关会拒绝 `credentials.*.value`、`clients.*.key` 和非环境
引用的 `key_ref`，避免明文凭据进入数据库/WAL/备份；无 Store 的本地开发仍可使用
literal 值。

如果配置中启用了可解析的 `clients.*.key_ref`/`key_env`，数据面会要求
`X-API-Key` 或 `Authorization: Bearer <client-key>`。本地开发可以不设置对应环境变量，
此时客户端鉴权保持关闭，但显式 profile 覆盖仍建议只在受保护网络中使用。

## 配置模型

`configs/gateway.yaml` 是一个可复制的最小示例。配置解析是**严格的**：未知键
（包括拼写错误）会在加载和 `make validate` 时直接报错并指出行号，而不是被静默
忽略。顶层对象如下：

- `listeners`：数据面、管理面和长连接监听地址；
- `upstreams`：上游名称、URL、协议、超时和认证引用；
- `profiles`：适配器、流量模式、上游以及请求/响应改写；
- `routes`：按 host/path/method/header/query/tenant/model 等条件选择 profile；
- `transforms`：可复用的声明式改写片段；
- `policies`：超时、限流、重试、熔断和审计策略；
- `credentials` / `clients`：上游密钥引用、客户端凭据和 profile 白名单；
- `observability`：日志级别、请求 ID 头、指标和 trace 设置。`log_level` 与
  `request_id_header` 已生效；`metrics: false` 可关闭 `/metrics` 端点（默认开启）；
  `/metrics` 在全局计数之外提供 `gateway_requests_by_route_total{route,profile,status}`
  序列，label 取自激活配置中的 route/profile 与状态类（2xx/4xx/…），基数有界；
  `redact_prompts: true` 会隐藏 preview/dry-run 中的请求正文；`trace` 仍为预留字段。

一个 profile 可以写成：

```yaml
profiles:
  user-v1-to-v2:
    adapter: transparent
    client_contract: generic-json
    upstream_contract: generic-json
    traffic_mode: live
    upstream: echo
    request:
      path: /v2/customers
      headers:
        set:
          X-Api-Version: "2"
        delete: [X-Debug]
      query:
        set:
          source: gateway
      body:
        rename:
          user_id: id
          name: displayName
        set:
          source: client
        delete: [debug]
    response:
      body:
        rename:
          id: user_id
          displayName: name
```

改写阶段按 `path -> query -> headers -> body` 的顺序执行。`body` 规则只在请求是
JSON 且 profile 声明了 JSON 契约时解析；未知字段默认保留。同一个规则映射内的
键按字典序执行；存在先后依赖时请用 `steps` 列表明确顺序，不依赖 YAML 映射的书写顺序。profile 级的
`max_body_bytes` 限制请求体大小（超限返回 413），同时约束上游响应的缓冲上限；
`allowed_methods` 可将 profile 限制到指定方法（其余返回 405）。`stream_safe` 是
三态：不写时按"是否有 body 改写规则"推导，显式写 `true`/`false` 时以配置为准
（`false` 表示整条流缓冲转换后再一次性返回）。

复杂规则可以抽成 `transforms`，在多个 profile 中复用：

```yaml
transforms:
  add-client-source:
    headers:
      set:
        X-Gateway-Source: unified
```

在 `request` / `response` 规则里写 `name: <片段名>` 即可引用，也可以用
`request_transforms` / `response_transforms` 列表串联多个片段；规则同样支持直接
内联。激活前会做 schema、引用、路由冲突和 adapter 能力校验：

```yaml
profiles:
  chat:
    request:
      name: add-client-source      # 引用上面的具名片段
    request_transforms:            # 或者以列表串联多个片段
      - name: add-client-source
      - path: /v2/chat
```

## 常用适配器与快速切换

适配器和运行模式是两个独立维度，同一适配器可以在 `live`、`dry-run`、`shadow`、
`mock`、`record` 或 `replay` 下运行。内置适配器包括：

| adapter | 用途 |
| --- | --- |
| `transparent` | HTTP/JSON 透传，只执行显式改写 |
| `openai-chat-to-anthropic` | OpenAI Chat Completions → Anthropic Messages |
| `anthropic-to-openai-chat` | Anthropic Messages → OpenAI Chat Completions |
| `openai-responses-to-anthropic` | OpenAI Responses → Anthropic Messages |

示例配置还包含一个 Vercel AI Gateway profile：`vercel-zai-glm-5-2`。它使用
OpenAI Chat Completions 契约，将上游模型固定为 `zai/glm-5.2`，并将出站
`User-Agent` 固定为 `eve/0.27.8`；API key 只从 `AI_GATEWAY_API_KEY` 环境变量读取。
该 profile 还会把过小的 `max_tokens` 提升到 128；这是为了兼容 Cherry Studio
连接检测发送的 `max_tokens=1`，避免 GLM 5.2 把全部额度消耗在 reasoning 后返回空正文。
透明 OpenAI/SSE 请求会逐块转发并立即 flush。声明 `stream_safe: true` 的语义转换
profile 同样逐帧翻译、逐帧 flush：上游的每个 SSE 事件在被接收的瞬间完成协议转换并发
给客户端，不会为了翻译而缓冲整条流。上游返回非流式错误体时自动回退到缓冲转换，客
户端仍能收到协议正确的错误响应。
启动服务前请在当前 shell 设置该变量，例如：

```powershell
$env:AI_GATEWAY_API_KEY = "<your-key>"
```

切换只需修改路由的 `profile`，或准备多个 profile 给不同租户使用：

```yaml
routes:
  - id: default-chat
    priority: 100
    match:
      path: /v1/chat/completions
      methods: [POST]
    profile: openai-to-anthropic
  - id: canary-chat
    priority: 200
    match:
      path: /v1/chat/completions
      methods: [POST]
      headers:
        X-Gateway-Profile: canary
    profile: openai-to-anthropic-dry-run
```

同一请求在进入数据面时锁定配置版本；热切换不会改变已经开始的请求。发布前可用
管理面 preview 查看最终改写后的请求（不提供逐阶段 diff），发布后可通过管理 API 回滚到上一版本。

## 运行模式

- `live`：正常访问上游并返回转换后的响应；
- `dry-run`：不建立上游连接，返回协议合法的模拟响应；管理面 preview 展示改写后的请求；
- `shadow`：复制到影子上游，客户端只接收主请求的结果；
- `mock`：使用 profile 中的固定/模板响应；
- `record`：在脱敏后保存请求、响应和配置版本 fixture；
- `replay`：根据请求摘要和 profile 版本回放 fixture。

`shadow`、`record`、`replay` 默认要求显式开启，避免意外副作用或重复计费。流式
响应开始发送后不能改变 HTTP 状态码；上游错误会转换为目标协议的错误事件并关闭
连接。非流式的上游错误（4xx/5xx）不会经过语义适配——网关保留原始状态码和上游
错误消息，按客户端协议的错误信封（OpenAI `{"error":{...}}` / Anthropic
`{"type":"error",...}`）返回，不会被伪装成空的"成功"响应。启用 `record` 时还必须
显式设置 `observability.record_body: true`。

record fixture 同时按 client、tenant 和完整配置版本 ID 隔离。切换到另一个
配置版本后不会跨版本误回放；需要回放时，可在同一配置中定义 `record` 与
`replay` 两个 profile，使用相同的 `name` 作为 fixture 命名空间，并用不同
route 选择它们。

`shadow` 模式可以用 profile 的 `shadow_upstream` 字段指定独立的影子上游；不指定时
镜像请求会发往主上游（注意双倍计费）。影子上限 64 个并发，超出即丢弃并记录警告，
且影子结果不影响熔断器。`replay` 先按请求摘要精确匹配录制；未命中时回退到同一
profile/method/path 最近的一条录制，并通过 `X-Gateway-Replay: exact|approximate`
响应头标注命中方式。

## 管理 API

管理面默认在 `127.0.0.1:8781`，建议通过反向代理或本机隧道访问。实现提供下列
稳定路径（返回 JSON；具体字段以当前版本为准）：

```text
GET  /healthz                       存活检查
GET  /readyz                        配置与存储就绪状态（不含上游探活）
GET  /admin/v1/status               当前激活版本、构建版本和指标摘要
GET  /admin/v1/config               当前配置（敏感字段已脱敏）
POST /admin/v1/config/validate      校验 YAML，不改变 active 版本
POST /admin/v1/preview              用样例请求返回最终改写后的请求预览
POST /admin/v1/config/activate      激活已校验版本
POST /admin/v1/config/rollback      回滚到指定历史版本
POST /admin/v1/routes/explain       解释一条请求会命中哪个 route/profile
GET  /admin/v1/recordings           查看 record fixture 索引
```

快速切换当前默认 profile：

```powershell
curl.exe -X POST http://127.0.0.1:8781/admin/v1/profile/switch `
  -H "Content-Type: application/json" `
  -d '{"profile":"openai-to-anthropic-dry-run"}'
```

一个最小的路由解释请求：

```powershell
curl.exe -X POST http://127.0.0.1:8781/admin/v1/routes/explain `
  -H "Content-Type: application/json" `
  -d '{"method":"POST","path":"/v1/chat/completions","headers":{"X-Gateway-Profile":["canary"]}}'
```

管理面和数据面使用不同的鉴权边界；生产部署不要把管理端口暴露到公网。
管理面默认仅允许回环客户端；设置 `GATEWAY_ADMIN_TOKEN` 后，CLI 或 Web 控制台需通过
`X-Admin-Token` 发送令牌。控制台静态页面可直接加载，配置及操作 API 仍需认证。
Web 控制台遇到 401 会提示输入或更换令牌并保存在当前浏览器；取消输入会清除原令牌。

`/admin/v1/versions`、`/admin/v1/audit`、`/admin/v1/recordings` 支持 `?limit=` 参数
（1–200，默认 100）。`/admin/v1/status` 返回 `gateway_version`（构建时通过
`-ldflags -X unified-api-gateway/internal/server.BuildVersion=...` 注入，`make build`
自动带入 git 描述）。注意：`/admin/v1/config/activate` 的 `path` 字段会以网关进程
权限读取本地文件，属于管理面信任边界，请勿将管理端口暴露给不受信任的调用方。

## 流量策略

`policies.defaults` 与顶层 `limits` 现在由数据面强制执行：

- `requests_per_second` / `burst`（或 `limits.requests_per_second`）：令牌桶限流，
  超限返回 `429` 并带 `Retry-After`；
- `max_concurrency`（或 `limits.concurrency`）：按 profile 的并发信号量，超限返回
  `503`；
- `retry.max_attempts` + `idempotent_only`：仅对幂等方法（GET/HEAD/PUT/DELETE 等）
  或携带 `Idempotency-Key` 的请求在 5xx/网络错误时重试；上游的 `retries` 字段可以
  提高重试次数上限，但幂等性判断始终生效（POST 不会被重放）；流式响应在首字节
  发出后绝不重试；
- `circuit_breaker.failure_threshold` / `reset_timeout_ms`：按 upstream 的熔断器，
  连续失败达到阈值后打开（`503 circuit_open`），静置期过后放行单个半开探测；熔断
  状态跨配置激活保留；
- `request_timeout_ms`：非流式上游调用的总超时；流式调用只限制响应头到达时间
  （30 秒），长生成不会被总超时截断。

`policies.audit.enabled: true` 时每个数据面请求写一条 `request` 审计事件（异步
写入 SQLite，积压时丢弃并告警）；`policies.audit.redact` 列表中的头名称会在预览、
dry-run 输出和 record 存档中额外脱敏。

`policies.defaults.require_client_auth: true` 时网关以 fail-closed 运行：未携带
有效客户端密钥的请求在三个平面（HTTP/WS/gRPC）一律 401，且启动时若没有任何
client 凭据能解析出密钥将直接拒绝启动，而不是退化成开放模式。
`policies.defaults.allow_anonymous_profile_override: false` 可以关闭开放模式下的
`X-Gateway-Profile` 越权入口（默认 true 以兼容旧行为）。credential `type: api_key`
将引用的环境变量以 `x-api-key` 头发送（Anthropic 的方案），`env` 则用
`Authorization: Bearer`。

Prometheus 分路由指标在每个进程中最多保留 1024 个 route/profile/status 组合。
超过预算的新组合累计到 `gateway_requests_route_overflow_total{status="..."}`，
已有序列持续累加；总请求计数不受影响。重启进程后计数与序列预算重新开始。

## 开发与测试

```powershell
make fmt
make test                # Go 测试与 Node.js 内置控制台交互测试（Node.js 22+）
make validate
```

服务运行时可用 CLI 热激活（连接不到管理面时会保存到 SQLite，供下一次启动使用）：

```powershell
go run ./cmd/gateway activate --config configs/gateway.yaml --admin-url http://127.0.0.1:8781
go run ./cmd/gateway health --admin-url http://127.0.0.1:8781
go run ./cmd/gateway version
```

测试夹具位于 `tests/fixtures`，每个 fixture 都标注输入契约、期望的上游 envelope
和响应契约。新增 adapter 时应至少添加：正常 JSON、流式/分块（若支持）、错误和
未知字段保留四组用例。运行单元测试不应访问真实上游；shadow/record 测试应使用
本地 httptest server。

## 安全边界

- 上游 URL 只来自已加载配置，不接受请求体或 Header 中的用户可控目标，降低 SSRF 风险；
- **未配置任何客户端密钥时，数据面处于开放模式**：任何人都可以通过
  `X-Gateway-Profile` 头选择任意 profile——这是该模式下唯一的越权入口。请仅在
  受保护网络中使用，或配置 `clients.*.key_ref`/`key_env`（对应环境变量存在时）
  强制客户端鉴权，使 profile 覆盖重新受白名单约束；
- Web 控制台只在管理面（默认 `127.0.0.1:8781`）提供服务；数据面 `/` 返回指向
  管理面的提示 JSON，避免把管理 API 调用误打到数据面；
- WebSocket/gRPC 面与数据面执行同一套准入控制（限流/并发/熔断）并计入指标与
  审计；WebSocket 升级做同源 Origin 校验，浏览器跨站连接被拒绝。WebSocket 使用
  独立的连接数预算（默认 256），长连接不会占满 HTTP 请求并发额度；
- 改写后的响应由网关重新分帧（Content-Length 自动重算），客户端不会读到
  与声明长度不符的截断响应；
- Authorization、API key、Cookie 等标准敏感头默认脱敏，`policies.audit.redact`
  可扩展需要脱敏的 header 名单；`redact_prompts` 隐藏 preview/dry-run 请求正文；
- `record` fixture 为了可回放会持久化转换后的响应正文；只应在受控环境显式开启
  `record_body`，不要录制不应落盘的数据；
- 客户端密钥与管理令牌使用常量时间比较，避免时序侧信道；
- 入站 `X-Request-Id`（或配置的 `observability.request_id_header`）只接受保守字符集，
  非法值会被替换为网关生成的 ID，防止日志/响应头注入；
- 内置 Web 控制台随二进制 embed 分发，响应带 CSP 与 nosniff 头，静态资源带 ETag；
- `observability.log_level`（debug/info/warn/error）控制访问日志级别：失败请求记
  info，成功请求记 debug；
- 自动重试只针对幂等请求或带明确幂等键的请求；首字节发送后不重试；
- 任意脚本（JavaScript/Lua/系统命令）不是默认转换能力；复杂扩展应使用受限、可
  审计的插件接口；
- 只代理用户有权访问的服务，不用于绕过供应商限制或伪造官方客户端身份。

## 已知限制

当前实现明确不做（或只做有限支持）的事情：

- SQLite 的 version 保存与 active marker 已在单事务内切换；运行中仍是“先发布完整
  内存 state、再提交重启 marker”，数据库提交失败会恢复旧内存 state；
- 交错的 OpenAI 并行工具参数增量会缓冲到流结束再按序重放（无损，但工具参数
  不再随到随发，且有单调用与整流总量上限）；
- Responses reasoning 的文本增量可保留为网关兼容 reasoning item；供应商专有
  summary/signature/redacted-thinking 语义仍可能降级；
- Responses 适配当前聚焦文本、图片和自定义 function call；内置 tools、远程 MCP、
  audio/refusal 等专用 item 不做语义翻译；
- gRPC 面当前是有界缓冲的 unary HTTP 透明代理，不实现 bidi 流，也不自带原生 h2c
  终结；需要部署在 HTTP/2/h2c 终结器之后；
- **readyz 检查配置与 SQLite 可用性，但不做上游探活**；
- `listeners` 地址与 `log_level` 不随热激活生效（需重启）；
- route 校验拒绝完全相同的 matcher+priority 组合；被高优先级路由完全遮蔽的路由
  会在激活和 `make validate` 时打印 warning（因 client profile 白名单仍可能使其
  可达，所以是警告而非错误）；glob/regex 等无法静态判定的包含关系不检测；
- 限流、并发与熔断状态保存在进程内存，SQLite 只承载版本历史与审计；多实例部署
  需要先把策略状态外置（共享限流/熔断层），当前版本按单机设计；
- 客户端鉴权默认在未配置任何可解析密钥时保持关闭（fail-open）；生产部署应
  设置 `require_client_auth: true` 强制 fail-closed，见上文流量策略。

## 项目布局

```text
cmd/gateway/       服务入口
internal/adapter/  协议 adapter 与 envelope
internal/config/   YAML schema、校验和版本快照
internal/server/   数据面、WebSocket/gRPC 与管理 API
internal/store/    SQLite 版本、审计和 record fixture
web/               内置控制台静态资源
configs/           可复制的示例配置
tests/fixtures/    adapter 与改写契约夹具
```
