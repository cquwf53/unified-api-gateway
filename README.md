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

也可以使用 Makefile 或 Docker：

```powershell
make run
make validate
docker build -t unified-api-gateway .
docker run --rm -p 8780:8780 -p 127.0.0.1:8781:8781 `
  -e ANTHROPIC_API_KEY=$env:ANTHROPIC_API_KEY `
  -e GATEWAY_ADMIN_TOKEN=$env:GATEWAY_ADMIN_TOKEN unified-api-gateway
```

如果当前系统没有 `make`，直接使用上面的 `go run` 命令即可。容器默认以只读配置
启动；需要热激活时，将自己的配置目录挂载到 `/app/configs`。

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

客户端密钥由网关验证；上游密钥只从环境变量或受保护的 secret 引用读取，不应写入
YAML 明文。

如果配置中启用了可解析的 `clients.*.key_ref`/`key_env`，数据面会要求
`X-API-Key` 或 `Authorization: Bearer <client-key>`。本地开发可以不设置对应环境变量，
此时客户端鉴权保持关闭，但显式 profile 覆盖仍建议只在受保护网络中使用。

## 配置模型

`configs/gateway.yaml` 是一个可复制的最小示例。顶层对象如下：

- `listeners`：数据面、管理面和长连接监听地址；
- `upstreams`：上游名称、URL、协议、超时和认证引用；
- `profiles`：适配器、流量模式、上游以及请求/响应改写；
- `routes`：按 host/path/method/header/query/tenant/model 等条件选择 profile；
- `transforms`：可复用的声明式改写片段；
- `policies`：超时、限流、重试、熔断和审计策略；
- `credentials` / `clients`：上游密钥引用、客户端凭据和 profile 白名单；
- `observability`：日志脱敏、指标和 trace 设置。

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
JSON 且 profile 声明了 JSON 契约时解析；未知字段默认保留。请求或响应体受到
`max_body_bytes` 限制，超限返回可识别的 413/转换错误，而不是静默截断。

复杂规则可以抽成 `transforms`，在多个 profile 中复用：

```yaml
transforms:
  add-client-source:
    headers:
      set:
        X-Gateway-Source: unified
```

`request.transforms`、`response.transforms` 可以引用名称，也可以直接写规则。激活
前会做 schema、引用、路由冲突和 adapter 能力校验。

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
透明 OpenAI/SSE 请求会逐块转发并立即 flush，不会等待完整响应体；需要语义转换的
profile 仍会按其 `streaming_safe` 声明选择逐帧转换或受控缓冲。
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
dry-run 预览每一阶段的 envelope diff，发布后可通过管理 API 回滚到上一版本。

## 运行模式

- `live`：正常访问上游并返回转换后的响应；
- `dry-run`：不建立上游连接，返回协议合法的模拟响应，并记录预览 diff；
- `shadow`：复制到影子上游，客户端只接收主请求的结果；
- `mock`：使用 profile 中的固定/模板响应；
- `record`：在脱敏后保存请求、响应和配置版本 fixture；
- `replay`：根据请求摘要和 profile 版本回放 fixture。

`shadow`、`record`、`replay` 默认要求显式开启，避免意外副作用或重复计费。流式
响应开始发送后不能改变 HTTP 状态码；上游错误会转换为目标协议的错误事件并关闭
连接。启用 `record` 时还必须显式设置 `observability.record_body: true`。

## 管理 API

管理面默认在 `127.0.0.1:8781`，建议通过反向代理或本机隧道访问。实现提供下列
稳定路径（返回 JSON；具体字段以当前版本为准）：

```text
GET  /healthz                       存活检查
GET  /readyz                        配置和上游就绪状态
GET  /admin/v1/status               当前激活版本、监听器和指标摘要
GET  /admin/v1/config               当前配置（敏感字段已脱敏）
POST /admin/v1/config/validate      校验 YAML，不改变 active 版本
POST /admin/v1/preview               用样例请求执行 dry-run 并返回 diff
POST /admin/v1/config/activate       激活已校验版本
POST /admin/v1/config/rollback       回滚到指定历史版本
POST /admin/v1/routes/explain        解释一条请求会命中哪个 route/profile
GET  /admin/v1/recordings             查看 record fixture 索引

快速切换当前默认 profile：

```powershell
curl.exe -X POST http://127.0.0.1:8781/admin/v1/profile/switch `
  -H "Content-Type: application/json" `
  -d '{"profile":"openai-to-anthropic-dry-run"}'
```
```

一个最小的路由解释请求：

```powershell
curl.exe -X POST http://127.0.0.1:8781/admin/v1/routes/explain `
  -H "Content-Type: application/json" `
  -d '{"method":"POST","path":"/v1/chat/completions","headers":{"X-Gateway-Profile":"canary"}}'
```

管理面和数据面使用不同的鉴权边界；生产部署不要把管理端口暴露到公网。
管理面默认仅允许回环客户端；设置 `GATEWAY_ADMIN_TOKEN` 后，CLI 或 Web 控制台需通过
`X-Admin-Token` 发送令牌。Web 控制台会在第一次收到 401 时提示输入并保存在当前浏览器。

## 开发与测试

```powershell
make fmt
make test
make validate
```

服务运行时可用 CLI 热激活（连接不到管理面时会保存到 SQLite，供下一次启动使用）：

```powershell
go run ./cmd/gateway activate --config configs/gateway.yaml --admin-url http://127.0.0.1:8781
```

测试夹具位于 `tests/fixtures`，每个 fixture 都标注输入契约、期望的上游 envelope
和响应契约。新增 adapter 时应至少添加：正常 JSON、流式/分块（若支持）、错误和
未知字段保留四组用例。运行单元测试不应访问真实上游；shadow/record 测试应使用
本地 httptest server。

## 安全边界

- 上游 URL 只来自已加载配置，不接受请求体或 Header 中的用户可控目标，降低 SSRF 风险；
- Authorization、API key、prompt、响应正文和工具参数默认脱敏；
- 自动重试只针对幂等请求或带明确幂等键的请求；首字节发送后不重试；
- 任意脚本（JavaScript/Lua/系统命令）不是默认转换能力；复杂扩展应使用受限、可
  审计的插件接口；
- 只代理用户有权访问的服务，不用于绕过供应商限制或伪造官方客户端身份。

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
