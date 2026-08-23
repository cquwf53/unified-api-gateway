# 测试夹具

夹具是纯 JSON 文件，不包含真实密钥或生产请求。它们用于 adapter、声明式改写和
dry-run 预览测试；测试应该在本地 `httptest.Server` 上运行，不能因为加载夹具而访问
互联网。

命名约定：

```text
<client-contract>-request.json       客户端输入
<client-contract>-upstream.json     期望发送给上游的 JSON
<client-contract>-response.json     上游输入（或网关返回）
<client-contract>-client.json       期望返回客户端的 JSON
```

当前样例：

- `generic-user-request.json` → `generic-user-upstream.json`：路径、Header 和 JSON
  字段改写；
- `openai-chat-request.json` → `anthropic-messages-upstream.json`：OpenAI Chat 到
  Anthropic Messages 的最小转换；
- `anthropic-messages-response.json` → `openai-chat-response.json`：响应字段和 usage
  归一化；
- `sse-anthropic-events.txt`：SSE 分块输入，包含 tool call 和 usage 事件。

新增夹具时请覆盖正常、未知字段、上游错误和（若 adapter 支持）流式四类场景。夹具
只描述数据契约，不描述密钥、网络地址或当前激活配置版本。
