(() => {
  const state = { status: null, profiles: [], routes: [], versions: [], audit: [], filter: "all" };
  const $ = (selector) => document.querySelector(selector);
  const $$ = (selector) => [...document.querySelectorAll(selector)];
  const escapeHTML = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[char]));
  const pretty = (value) => JSON.stringify(value, null, 2);
  const time = (value) => value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "—";
  const short = (value, length = 12) => { const text = String(value ?? ""); return text.length > length ? `${text.slice(0, length)}…` : text; };

  let authRevision = 0;
  let tokenPrompt = null;

  async function api(path, options = {}) {
    const revision = authRevision;
    const token = window.localStorage.getItem("gateway_admin_token");
    const request = (adminToken) => {
      const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
      if (adminToken) headers["X-Admin-Token"] = adminToken;
      return fetch(path, { ...options, headers });
    };
    let response = await request(token);
    if (response.status === 401) {
      // Requests from one refresh share the prompt, including late 401s from
      // the old token. A canceled or rejected token is never left in storage.
      if (revision === authRevision && !tokenPrompt) {
        authRevision++;
        window.localStorage.removeItem("gateway_admin_token");
        tokenPrompt = Promise.resolve().then(() => {
          const entered = window.prompt("请输入管理面令牌（留空取消并清除原令牌）")?.trim();
          if (entered) window.localStorage.setItem("gateway_admin_token", entered);
          return entered;
        }).finally(() => { tokenPrompt = null; });
      }
      const replacement = tokenPrompt ? await tokenPrompt : window.localStorage.getItem("gateway_admin_token");
      if (replacement) {
        response = await request(replacement);
        if (response.status === 401 && window.localStorage.getItem("gateway_admin_token") === replacement) {
          window.localStorage.removeItem("gateway_admin_token");
          authRevision++;
        }
      }
    }
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data?.error?.message || data?.message || `请求失败（${response.status}）`);
    return data;
  }

  function toast(message, isError = false) {
    const node = $("#toast");
    node.textContent = message;
    node.classList.toggle("toast-error", isError);
    node.classList.add("toast-show");
    window.clearTimeout(toast.timer);
    toast.timer = window.setTimeout(() => node.classList.remove("toast-show"), 3200);
  }

  function setConnection(connected) {
    const node = $(".connection");
    node.classList.toggle("connected", connected);
    node.classList.toggle("error", !connected);
    $("#connection-text").textContent = connected ? "控制面已连接" : "控制面不可用";
  }

  function renderStatus() {
    const snapshot = state.status?.config;
    const metrics = state.status?.metrics || {};
    $("#metric-total").textContent = metrics.requests_total ?? 0;
    $("#metric-ok").textContent = metrics.requests_ok ?? 0;
    $("#metric-active").textContent = metrics.active_requests ?? 0;
    $("#metric-adapters").textContent = state.status?.adapters?.length ?? 0;
    const total = Number(metrics.requests_total || 0);
    const ok = Number(metrics.requests_ok || 0);
    $("#metric-rate").textContent = total ? `${Math.round(ok / total * 100)}% 成功率` : "— 成功率";
    $("#active-version").textContent = snapshot?.id ? `配置 ${short(snapshot.id, 16)}` : "未加载配置";
    $("#snapshot-id").textContent = snapshot?.id || "—";
    $("#snapshot-time").textContent = time(snapshot?.loaded_at);
    $("#snapshot-profiles").textContent = state.profiles.length;
    $("#snapshot-routes").textContent = state.routes.length;
    const recent = metrics.recent || [];
    $("#recent-requests").innerHTML = recent.length ? recent.map((item) => `
      <tr><td><span class="request-method">${escapeHTML(item.method)}</span><span class="request-path">${escapeHTML(item.path)}</span></td>
      <td>${escapeHTML(item.profile || "—")}</td><td><span class="mode-tag ${escapeHTML(item.mode)}">${escapeHTML(item.mode || "live")}</span></td>
      <td class="${!item.error_code && ((item.status >= 200 && item.status < 400) || item.status === 101) ? "status-ok" : "status-fail"}">${escapeHTML(item.status || "—")}</td><td>${item.duration_ns ? `${(item.duration_ns / 1e6).toFixed(1)} ms` : "—"}</td></tr>`).join("") : '<tr><td colspan="5" class="empty">尚无请求</td></tr>';
  }

  function renderProfiles() {
    const profiles = state.filter === "all" ? state.profiles : state.profiles.filter((profile) => profile.traffic_mode === state.filter);
    $("#profile-grid").innerHTML = profiles.length ? profiles.map((profile) => {
      const mode = profile.traffic_mode || "live";
      const icon = profile.adapter?.includes("openai") ? "✦" : profile.adapter?.includes("anthropic") ? "◉" : "⇄";
      return `<article class="profile-card"><div class="profile-card-header"><div><h3 title="${escapeHTML(profile.id)}">${escapeHTML(profile.name || profile.id)}</h3><span class="contract">${escapeHTML(profile.client_contract || "generic")}</span></div><div class="adapter-icon">${icon}</div></div><div class="profile-meta"><div><span>Adapter</span><b title="${escapeHTML(profile.adapter)}">${escapeHTML(profile.adapter || "transparent")}</b></div><div><span>Upstream</span><b>${escapeHTML(profile.upstream || "local")}</b></div><div><span>流式</span><b>${profile.stream_safe ? "逐帧安全" : "受控缓冲"}</b></div></div><div class="profile-footer"><span class="mode-label ${escapeHTML(mode)}">● ${escapeHTML(mode)}</span><button class="mini-button" data-switch-profile="${escapeHTML(profile.id)}">设为默认</button></div></article>`;
    }).join("") : '<div class="loading-card">没有符合条件的 profile</div>';
    const select = $("#preview-profile");
    const previous = select.value;
    select.innerHTML = state.profiles.map((p) => `<option value="${escapeHTML(p.id)}">${escapeHTML(p.id)} · ${escapeHTML(p.adapter || "transparent")}</option>`).join("");
    if (state.profiles.some((p) => p.id === previous)) select.value = previous;
  }

  function renderRoutes() {
    $("#route-list").innerHTML = state.routes.length ? state.routes.map((route) => {
      const match = route.match || {};
      const matcher = [match.methods?.join("|") || match.method, match.path || (match.path_prefix ? `${match.path_prefix}*` : "*"), match.headers && Object.keys(match.headers).length ? `header:${Object.keys(match.headers).join(",")}` : ""].filter(Boolean).join("  ");
      return `<div class="route-row"><div class="route-priority">${escapeHTML(route.priority || 0)}</div><div class="route-name">${escapeHTML(route.name || route.id || "route")}</div><div class="route-match">${escapeHTML(matcher)}</div><div class="route-profile"><span>${escapeHTML(route.profile)}</span><button class="mini-button route-switch" data-switch-route="${escapeHTML(route.id || route.name || "")}">切换</button></div></div>`;
    }).join("") : '<div class="empty">没有路由规则</div>';
  }

  function renderVersions() {
    $("#version-list").innerHTML = state.versions.length ? state.versions.map((version) => `<div class="version-item ${version.active ? "active" : ""}"><div class="version-main"><b>${escapeHTML(version.id)}</b><span>${escapeHTML(version.description || "无说明")} · ${time(version.created_at)} · ${escapeHTML(version.created_by || "system")}</span></div><div class="version-actions">${version.active ? '<span class="active-label">ACTIVE</span>' : `<button class="mini-button" data-rollback="${escapeHTML(version.id)}">回滚</button>`}</div></div>`).join("") : '<div class="empty">暂无版本</div>';
    $("#audit-list").innerHTML = state.audit.length ? state.audit.slice(0, 8).map((event) => `<div class="audit-item"><b>${escapeHTML(event.action)}</b><span>${escapeHTML(event.actor || "system")} · ${time(event.created_at)}</span></div>`).join("") : '<div class="empty">暂无审计事件</div>';
  }

  async function refresh() {
    try {
      const [status, profiles, routes, versions, audit] = await Promise.all([api("/api/status"), api("/api/profiles"), api("/api/routes"), api("/api/versions"), api("/api/audit")]);
      state.status = status; state.profiles = profiles.profiles || []; state.routes = routes.routes || []; state.versions = versions.versions || []; state.audit = audit.events || [];
      setConnection(true); renderStatus(); renderProfiles(); renderRoutes(); renderVersions();
      return true;
    } catch (error) { setConnection(false); toast(error.message, true); return false; }
  }

  function switchView(view) {
    $$(".nav-item").forEach((item) => item.classList.toggle("active", item.dataset.view === view));
    $$(".view").forEach((item) => item.classList.toggle("active", item.id === `view-${view}`));
    const titles = { overview: "运行概览", profiles: "适配器", routes: "路由规则", playground: "改写实验室", versions: "版本与审计" };
    $("#page-title").textContent = titles[view] || "运行概览";
  }

  $$(".nav-item").forEach((item) => item.addEventListener("click", () => switchView(item.dataset.view)));
  $("#refresh-button").addEventListener("click", async () => { if (await refresh()) toast("状态已刷新"); });
  $$("[data-profile-filter]").forEach((button) => button.addEventListener("click", () => { state.filter = button.dataset.profileFilter; $$("[data-profile-filter]").forEach((item) => item.classList.toggle("active", item === button)); renderProfiles(); }));

  $("#preview-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const output = $("#preview-output");
    try {
      const headers = JSON.parse($("#preview-headers").value || "{}");
      const body = JSON.parse($("#preview-body").value || "null");
      output.textContent = "正在执行 dry-run…";
      const result = await api("/api/preview", { method: "POST", body: JSON.stringify({ profile: $("#preview-profile").value, method: $("#preview-method").value, path: $("#preview-path").value, headers, body }) });
      output.textContent = pretty(result);
      toast("Dry-run 完成，未访问上游");
    } catch (error) { output.textContent = error.message; toast(error.message, true); }
  });
  $("#copy-preview").addEventListener("click", async () => { try { await navigator.clipboard.writeText($("#preview-output").textContent); toast("已复制预览结果"); } catch { toast("复制失败", true); } });
  $("#explain-demo").addEventListener("click", async () => { try { const data = await api("/api/explain?path=/v1/chat/completions&method=POST"); toast(`命中 ${data.profile?.name || "默认规则"}`); } catch (error) { toast(error.message, true); } });

  const dialog = $("#activate-dialog");
  $$("[data-open-activate]").forEach((button) => button.addEventListener("click", () => {
    $("#activate-yaml").value = "";
    $("#activate-description").value = "";
    dialog.showModal();
  }));
  $$("[data-close-activate]").forEach((button) => button.addEventListener("click", () => dialog.close()));
  $("#activate-form").addEventListener("submit", async (event) => { event.preventDefault(); if (event.submitter?.value === "cancel") { dialog.close(); return; } try { const yaml = $("#activate-yaml").value.trim(); if (!yaml) throw new Error("YAML 不能为空"); const result = await api("/api/activate", { method: "POST", body: JSON.stringify({ yaml, description: $("#activate-description").value }) }); dialog.close(); toast(`配置已激活：${result.id}`); await refresh(); } catch (error) { toast(error.message, true); } });
  $("#version-list").addEventListener("click", async (event) => { const button = event.target.closest("[data-rollback]"); if (!button) return; if (!window.confirm(`确认回滚到 ${button.dataset.rollback}？`)) return; try { await api("/api/rollback", { method: "POST", body: JSON.stringify({ id: button.dataset.rollback }) }); toast("已回滚配置"); await refresh(); } catch (error) { toast(error.message, true); } });
  $("#profile-grid").addEventListener("click", async (event) => { const button = event.target.closest("[data-switch-profile]"); if (!button) return; if (!window.confirm(`将 ${button.dataset.switchProfile} 设为默认 profile？`)) return; try { const result = await api("/api/switch", { method: "POST", body: JSON.stringify({ profile: button.dataset.switchProfile, description: `quick switch → ${button.dataset.switchProfile}` }) }); toast(`已切换：${result.profile}`); await refresh(); } catch (error) { toast(error.message, true); } });
  $("#route-list").addEventListener("click", async (event) => { const button = event.target.closest("[data-switch-route]"); if (!button) return; const profile = window.prompt("输入要切换到的 profile ID：", state.routes.find((route) => (route.id || route.name) === button.dataset.switchRoute)?.profile || ""); if (!profile) return; try { await api("/api/switch", { method: "POST", body: JSON.stringify({ route_id: button.dataset.switchRoute, profile, description: `route switch → ${profile}` }) }); toast("路由 profile 已切换"); await refresh(); } catch (error) { toast(error.message, true); } });

  refresh();
  window.setInterval(refresh, 10000);
})();
