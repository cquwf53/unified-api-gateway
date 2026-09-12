const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

// Exercise the shipped script through its DOM events. The small DOM below
// models button/form default submission, including buttons with no type.
async function consolePage({ token, promptReplies = [], respond } = {}) {
  const nodes = [];
  const storage = new Map(token ? [["gateway_admin_token", token]] : []);
  const requests = [];
  const prompts = [];
  let form = null;
  class Element {
    constructor(tag, attrs) {
      this.tagName = tag.toUpperCase();
      this.attrs = attrs;
      this.id = attrs.id;
      this.form = form;
      this.value = attrs.value || "";
      this.textContent = "";
      this.innerHTML = "";
      this.open = false;
      this.listeners = new Map();
      this.dataset = Object.fromEntries(Object.entries(attrs).filter(([key]) => key.startsWith("data-")).map(([key, value]) => [key.slice(5).replace(/-([a-z])/g, (_, char) => char.toUpperCase()), value]));
      const classes = new Set((attrs.class || "").split(/\s+/));
      this.classList = {
        add: (name) => classes.add(name), remove: (name) => classes.delete(name), contains: (name) => classes.has(name),
        toggle(name, force) { const on = force ?? !classes.has(name); if (on) classes.add(name); else classes.delete(name); return on; },
      };
    }
    addEventListener(type, listener) { this.listeners.set(type, [...(this.listeners.get(type) || []), listener]); }
    async dispatch(type, values = {}) {
      const event = { target: this, currentTarget: this, defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }, ...values };
      for (const listener of this.listeners.get(type) || []) await listener(event);
      return event;
    }
    async click() {
      const event = await this.dispatch("click");
      if (!event.defaultPrevented && this.tagName === "BUTTON" && this.form && (this.attrs.type || "submit") === "submit") {
        const submission = await this.form.dispatch("submit", { submitter: this });
        if (!submission.defaultPrevented && this.form.attrs.method === "dialog") find("#activate-dialog").close();
      }
      await settle();
    }
    closest(selector) { return matches(this, selector) ? this : null; }
    showModal() { this.open = true; }
    close() { this.open = false; }
  }
  function matches(node, selector) {
    if (selector.startsWith("#")) return node.id === selector.slice(1);
    if (selector.startsWith(".")) return node.classList.contains(selector.slice(1));
    const attribute = /^\[([^=\]]+)(?:="([^"]*)")?\]$/.exec(selector);
    return attribute && Object.hasOwn(node.attrs, attribute[1]) && (attribute[2] === undefined || node.attrs[attribute[1]] === attribute[2]);
  }
  function find(selector) { const node = nodes.find((item) => matches(item, selector)); assert.ok(node, `element ${selector} exists`); return node; }
  const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");
  for (const match of html.matchAll(/<(\/?)([a-z][\w-]*)([^>]*)>/gi)) {
    if (match[1]) { if (match[2] === "form") form = null; continue; }
    const attrs = Object.fromEntries([...match[3].matchAll(/([\w-]+)(?:="([^"]*)")?/g)].map((entry) => [entry[1], entry[2] || ""]));
    const node = new Element(match[2], attrs);
    nodes.push(node);
    if (match[2] === "form") form = node;
  }
  const defaults = {
    "/api/status": { config: { id: "test-config" }, metrics: {}, adapters: [] },
    "/api/profiles": { profiles: [{ id: "preview", adapter: "transparent", traffic_mode: "dry-run" }] },
    "/api/routes": { routes: [] }, "/api/versions": { versions: [] }, "/api/audit": { events: [] },
    "/api/config": { version: 1 }, "/api/activate": { id: "activated" },
  };
  const context = {
    document: { querySelector: find, querySelectorAll: (selector) => nodes.filter((node) => matches(node, selector)) },
    navigator: { clipboard: { writeText: async () => {} } },
    window: {
      localStorage: { getItem: (key) => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value), removeItem: (key) => storage.delete(key) },
      prompt: (message) => { prompts.push(message); return promptReplies.shift() ?? null; },
      confirm: () => true, setTimeout: () => 1, clearTimeout: () => {}, setInterval: () => 1,
    },
    fetch: async (url, options = {}) => {
      requests.push({ url, ...options });
      const supplied = respond ? await respond(url, options) : null;
      const status = supplied?.status ?? 200;
      return { status, ok: status >= 200 && status < 300, json: async () => supplied?.body ?? defaults[url] ?? {} };
    },
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, "app.js"), "utf8"), context, { filename: "app.js" });
  await settle();
  return { find, nodes, storage, requests, prompts };
}
async function settle() { await new Promise(setImmediate); await new Promise(setImmediate); }

test("cancel and close never submit an activation, including after pasting YAML", async () => {
  for (const selector of ['[aria-label="关闭"]', '[value="cancel"]']) {
    const page = await consolePage();
    const cancel = selector === '[value="cancel"]' ? page.nodes.find((node) => node.attrs.value === "cancel" && !node.attrs["aria-label"]) : page.find(selector);
    await page.find("[data-open-activate]").click();
    page.find("#activate-yaml").value = "version: 1\nprofiles: {}";
    await cancel.click();
    assert.equal(page.find("#activate-dialog").open, false);
    assert.equal(page.requests.filter((request) => request.url === "/api/activate").length, 0);
  }
});

test("opening the dialog then submitting without YAML cannot replace configuration", async () => {
  const page = await consolePage();
  await page.find("[data-open-activate]").click();
  await page.find('[value="default"]').click();
  assert.equal(page.requests.filter((request) => request.url === "/api/activate").length, 0);
  assert.equal(page.find("#activate-dialog").open, true);
});

test("explicit activation sends the pasted YAML once", async () => {
  const page = await consolePage();
  await page.find("[data-open-activate]").click();
  const yaml = "version: 1\nprofiles: {}";
  page.find("#activate-yaml").value = yaml;
  await page.find('[value="default"]').click();
  const activations = page.requests.filter((request) => request.url === "/api/activate");
  assert.equal(activations.length, 1);
  assert.equal(JSON.parse(activations[0].body).yaml, yaml);
  assert.equal(page.find("#activate-dialog").open, false);
});

test("concurrent unauthorized requests share one replacement-token prompt", async () => {
  const page = await consolePage({ token: "expired", promptReplies: ["fresh"], respond: (_, options) => options.headers["X-Admin-Token"] === "fresh" ? null : { status: 401 } });
  assert.equal(page.prompts.length, 1);
  assert.equal(page.storage.get("gateway_admin_token"), "fresh");
  assert.equal(page.find("#connection-text").textContent, "控制面已连接");
});

test("canceling token replacement removes the invalid stored token", async () => {
  const page = await consolePage({ token: "expired", respond: () => ({ status: 401 }) });
  assert.equal(page.prompts.length, 1);
  assert.equal(page.storage.has("gateway_admin_token"), false);
});

test("a failed refresh never displays a success toast", async () => {
  const page = await consolePage({ respond: () => ({ status: 503, body: { error: { message: "控制面维护中" } } }) });
  await page.find("#refresh-button").click();
  assert.equal(page.find("#toast").textContent, "控制面维护中");
});

test("successful WebSocket upgrades use the success presentation", async () => {
  const page = await consolePage({ respond: (url) => url === "/api/status" ? { body: { metrics: { recent: [{ status: 101, method: "GET", path: "/ws" }] } } } : null });
  assert.match(page.find("#recent-requests").innerHTML, /class="status-ok">101/);
});

test("a cached cancel submit event cannot activate configuration", async () => {
  const page = await consolePage();
  await page.find("[data-open-activate]").click();
  page.find("#activate-yaml").value = "version: 1";
  await page.find("#activate-form").dispatch("submit", { submitter: { value: "cancel" } });
  assert.equal(page.requests.filter((request) => request.url === "/api/activate").length, 0);
  assert.equal(page.find("#activate-dialog").open, false);
});

test("a rejected replacement token is cleared after one retry", async () => {
  const page = await consolePage({ token: "expired", promptReplies: ["incorrect"], respond: () => ({ status: 401 }) });
  assert.equal(page.prompts.length, 1);
  assert.equal(page.storage.has("gateway_admin_token"), false);
  assert.equal(page.find("#connection-text").textContent, "控制面不可用");
});

test("a late unauthorized response reuses the token entered by earlier requests", async () => {
  let release;
  const page = await consolePage({ token: "expired", promptReplies: ["fresh"], respond: (url, options) => {
    if (options.headers["X-Admin-Token"] === "fresh") return null;
    if (url === "/api/audit") return new Promise((resolve) => { release = () => resolve({ status: 401 }); });
    return { status: 401 };
  } });
  release();
  await settle();
  assert.equal(page.prompts.length, 1);
  assert.equal(page.find("#connection-text").textContent, "控制面已连接");
});
