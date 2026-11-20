// SPDX-License-Identifier: Apache-2.0
// Public snapshot: remove the deployment-specific default relay address.
(() => {
  "use strict";
  if (document.getElementById("btn-hosting")) return;
  const tools = document.querySelector("#tb-tools .tb-in"), more = document.getElementById("btn-more");
  if (!tools || !more) return;

  const el = (tag, cls, value) => {
    const node = document.createElement(tag);
    if (cls) node.className = cls;
    if (value !== undefined) node.textContent = String(value);
    return node;
  };
  const button = (label, act, cls = "") => {
    const b = el("button", "cch-btn " + cls, label); b.type = "button";
    if (act) b.addEventListener("click", act);
    return b;
  };
  const field = (label, input) => { const box = el("label", "cch-field"); box.append(el("span", "", label), input); return box; };
  const input = (type = "text", placeholder = "", value = "") => {
    const n = el("input"); n.type = type; n.placeholder = placeholder; n.value = value;
    n.autocomplete = "off"; n.spellcheck = false; return n;
  };
  const note = (s) => el("p", "cch-note", s);
  const row = (...nodes) => { const n = el("div", "cch-row"); n.append(...nodes); return n; };
  const card = (title) => { const n = el("section", "cch-card"); if (title) n.append(el("h3", "", title)); return n; };
  const id = () => [...crypto.getRandomValues(new Uint8Array(16))].map((v) => v.toString(16).padStart(2, "0")).join("");
  const arr = (v) => Array.isArray(v) ? v : [];
  const brief = (v, max = 300) => String(v || "").slice(0, max);
  const displayJson = (v) => { try { return JSON.stringify(v, null, 2) || ""; } catch { return "（无法显示）"; } };

  const trigger = button("", () => open(), "");
  trigger.id = "btn-hosting"; trigger.className = "icon-btn";
  trigger.title = "会话托管"; trigger.setAttribute("aria-label", "会话托管");
  trigger.setAttribute("aria-haspopup", "dialog");
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg"); svg.setAttribute("viewBox", "0 0 24 24");
  const shape = document.createElementNS("http://www.w3.org/2000/svg", "path");
  shape.setAttribute("d", "M4 8h16l-4-4M20 16H4l4 4");
  svg.append(shape); trigger.append(svg);
  // 入口收在 ⋯ 的工具区；上游重排按钮后仍放在末尾，不占常驻顶栏。
  const placeTrigger = () => {
    if (tools.lastElementChild !== trigger) tools.append(trigger);
    more.classList.remove("tb-off");
    [...tools.querySelectorAll(".icon-btn:not(.hidden):not(.tb-off)")].reverse().forEach((b, i) => b.style.setProperty("--i", i));
  };
  placeTrigger();
  document.addEventListener("cc:ui", (e) => { if (e.detail.key === "topbar") placeTrigger(); });

  const panel = el("dialog", "cch-panel hidden"); panel.id = "cch-panel";
  panel.setAttribute("aria-labelledby", "cch-title");
  const top = el("header", "cch-top"), title = el("h2", "", "会话托管"); title.id = "cch-title";
  const closeBtn = button("关闭", () => close(), "cch-quiet");
  top.append(title, closeBtn);
  const tabs = el("nav", "cch-tabs"); tabs.setAttribute("aria-label", "托管功能");
  const hostTab = button("此设备作为主机", () => navigate("host"));
  const remoteTab = button("连接其他设备", () => navigate("remote")); tabs.append(hostTab, remoteTab);
  const notice = el("div", "cch-notice"); notice.hidden = true; notice.setAttribute("role", "status");
  const body = el("div", "cch-body");
  panel.append(top, tabs, notice, body); document.body.append(panel);

  const state = { open: false, pageOpen: false, tab: "host", gen: 0, timer: 0, watchdog: 0, writeEpoch: 0, ctrls: new Set(), status: null, remote: null,
    remoteList: null, session: null, pendingOpen: "", online: false, seen: 0, busy: false, fetching: false, native: null, hostUI: null,
    savedFocus: null, lastError: "", readEpoch: 0, readCtrl: null, revision: "", negotiated: false, confirmed: false };
  function cancelRead() {
    state.readEpoch++; state.readCtrl?.abort(); state.readCtrl = null; state.fetching = false;
  }
  const backgroundCtrls = new Set();
  let backgroundEpoch = 0;
  function cancelBackground() {
    backgroundEpoch++;
    if (sideBusy) sideInvalid = true;   // 有设备没读完，下次真正看得到侧栏时补读
    sideBusy = false;
    for (const c of backgroundCtrls) c.abort();
    backgroundCtrls.clear();
  }
  const current = (g) => state.pageOpen && g === state.gen;
  // 配置窗格与聊天来源各有请求世代；在平板右栏改配置不会重置主机聊天的 watch。
  const config = { open: false, tab: "host", gen: 0, writeEpoch: 0, ctrls: new Set(), timer: 0, busy: false, fetching: false, loading: false,
    status: null, remote: null, remoteList: null, hostUI: null, savedFocus: null, busyButton: null, mode: "", online: false, error: "",
    read: null, covered: false, needsInit: false };
  const configReadable = () => config.open && !config.covered && !document.hidden;
  function cancelConfigRead() {
    clearTimeout(config.timer); config.timer = 0;
    config.read?.abort(); config.read = null; config.fetching = false;
  }
  function resumeConfigRead() {
    if (!configReadable() || config.busy) return;
    if (config.needsInit) navigate(config.tab, config.remote); else scheduleConfig(0);
  }
  const configCurrent = (g) => config.open && g === config.gen;
  const showNotice = (message, error = false) => {
    if (!config.open) { if (message) window.__cc?.toast?.(message, error); return; }
    notice.textContent = message || ""; notice.hidden = !message; notice.classList.toggle("cch-error", error);
  };
  function cancelView() {
    cancelRead(); state.revision = ""; state.negotiated = false; state.confirmed = false;
    state.gen++; clearTimeout(state.timer); state.timer = 0;
    clearTimeout(state.watchdog); state.watchdog = 0;
    for (const c of state.ctrls) c.abort(); state.ctrls.clear();
    state.fetching = false; state.online = false; state.seen = 0; state.busy = false;
    state.lastError = "";
  }
  async function api(path, payload, g = state.gen, signal) {
    const ctl = new AbortController(); state.ctrls.add(ctl);
    const cancel = () => ctl.abort();
    if (signal?.aborted) ctl.abort(); else signal?.addEventListener("abort", cancel, { once: true });
    const timeout = setTimeout(() => ctl.abort(), 32000);
    try {
      const r = await fetch("/api/hosting/" + path, { method: payload === undefined ? "GET" : "POST",
        credentials: "same-origin", headers: { "Content-Type": "application/json", "X-CC-Lang": "zh" },
        ...(payload === undefined ? {} : { body: JSON.stringify(payload) }), signal: ctl.signal });
      if (ctl.signal.aborted || !current(g)) throw new Error("界面已切换");
      const out = await r.json();
      if (ctl.signal.aborted || !current(g)) throw new Error("界面已切换");
      if (!r.ok) throw new Error(brief(out.error || out.message, 400) || `请求失败（${r.status}）`);
      return out;
    } catch (e) {
      if (e.name === "AbortError") throw new Error("请求超时或已取消，请刷新确认主机状态");
      throw e;
    } finally { clearTimeout(timeout); signal?.removeEventListener("abort", cancel); state.ctrls.delete(ctl); }
  }
  function setTabs() {
    hostTab.setAttribute("aria-current", config.tab === "host" ? "page" : "false");
    remoteTab.setAttribute("aria-current", config.tab === "remote" ? "page" : "false");
  }
  function configLayout({ wide, top: topmost }) {
    if (!config.open) return;
    const mode = wide ? "pane" : "modal";
    if (mode !== config.mode) {
      const focused = document.activeElement;
      if (panel.open) panel.close();
      panel.inert = false; config.mode = mode; panel.dataset.mode = mode;
      panel.setAttribute("aria-modal", String(!wide));
      if (wide) panel.show(); else panel.showModal();
      if (focused?.isConnected && (wide || panel.contains(focused))) focused.focus({ preventScroll: true });
      else if (!wide) closeBtn.focus({ preventScroll: true });
    }
    panel.inert = wide && !topmost;
    if (panel.inert && panel.contains(document.activeElement)) document.activeElement.blur();
    if (config.covered !== panel.inert) {
      config.covered = panel.inert;
      if (config.covered) cancelConfigRead(); else resumeConfigRead();
    }
  }
  window.CCLayout?.registerPane?.("cch-panel", "托管", { back: configBack, layout: configLayout });
  function open() {
    if (config.open) { window.CCLayout?.raise?.("cch-panel"); return; }
    config.savedFocus = document.activeElement; config.open = true;
    panel.classList.remove("hidden");
    window.CCLayout?.raise?.("cch-panel");
    configLayout({ wide: (window.CCLayout?.tier?.() || 1) >= 2, top: true });
    trigger.setAttribute("aria-expanded", "true");
    navigate(config.tab); closeBtn.focus({ preventScroll: true });
  }
  function cancelConfig() {
    config.gen++; cancelConfigRead(); config.needsInit = false;
    for (const c of config.ctrls) c.abort(); config.ctrls.clear();
    if (config.busyButton?.isConnected) config.busyButton.disabled = false; config.busyButton = null;
    config.busy = false; config.fetching = false; config.loading = false; config.hostUI = null;
    config.online = false; config.error = "";
  }
  function close() {
    if (!config.open) return;
    const restore = panel.contains(document.activeElement);
    cancelConfig(); config.open = false; config.covered = false; config.mode = "";
    body.replaceChildren(); notice.textContent = ""; notice.hidden = true;
    panel.close(); panel.inert = false; panel.classList.add("hidden");
    trigger.setAttribute("aria-expanded", "false"); window.CCLayout?.apply?.();
    if (restore) try { (config.savedFocus?.isConnected && config.savedFocus.getClientRects().length ? config.savedFocus : trigger).focus({ preventScroll: true }); } catch {}
    refreshRemoteIndex(true);   // 面板里可能刚配对 / 撤销托管，侧栏跟着刷新
  }
  function configBack() {
    if (!config.open) return false;
    if (config.remote) navigate("remote"); else close();
    return true;
  }
  const previousBack = window.__ccBack;
  window.__ccBack = function (...args) {
    if (config.open && config.mode === "modal") return configBack();
    if (typeof previousBack === "function" && previousBack.apply(this, args)) return true;
    if (state.pageOpen) { closeRemotePage(); return true; }
    return false;
  };
  panel.addEventListener("cancel", (e) => { e.preventDefault(); e.stopPropagation(); configBack(); });
  panel.addEventListener("keydown", (e) => {
    if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); configBack(); }
    if (e.key !== "Tab" || config.mode !== "modal") return;
    const focusable = [...panel.querySelectorAll("button,input,textarea,select,summary,[tabindex]")]
      .filter((n) => !n.disabled && !n.closest("[hidden]") && n.getClientRects().length);
    if (!focusable.length) return;
    const first = focusable[0], last = focusable[focusable.length - 1];
    if (e.shiftKey && (document.activeElement === first || !panel.contains(document.activeElement))) { e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
  });

  async function configApi(path, payload, g = config.gen, ctl = new AbortController()) {
    config.ctrls.add(ctl);
    const timeout = setTimeout(() => ctl.abort(), 32000);
    try {
      const r = await fetch("/api/hosting/" + path, { method: payload === undefined ? "GET" : "POST", credentials: "same-origin",
        headers: { "Content-Type": "application/json", "X-CC-Lang": "zh" }, ...(payload === undefined ? {} : { body: JSON.stringify(payload) }), signal: ctl.signal });
      if (ctl.signal.aborted || !configCurrent(g)) throw new Error("界面已切换");
      const out = await r.json();
      if (ctl.signal.aborted || !configCurrent(g)) throw new Error("界面已切换");
      if (!r.ok) throw new Error(brief(out.error || out.message, 400) || `请求失败（${r.status}）`);
      return out;
    } catch (e) {
      if (e.name === "AbortError") throw new Error("请求超时或已取消，请刷新确认主机状态");
      throw e;
    } finally { clearTimeout(timeout); config.ctrls.delete(ctl); }
  }
  async function navigate(tab, remote = null) {
    cancelConfig(); config.tab = tab; config.remote = remote; config.remoteList = null; config.loading = true;
    setTabs(); showNotice(""); body.replaceChildren(note("正在读取托管配置…"));
    const g = config.gen;
    config.needsInit = true;
    if (!configReadable()) return;
    const ctl = config.read = new AbortController();
    try {
      const status = await configApi("status", undefined, g, ctl);
      if (!configCurrent(g) || config.read !== ctl || !configReadable()) return;
      config.status = status; config.loading = config.needsInit = false;
      if (tab === "host") renderHost(); else if (remote) renderRemoteSessions(); else renderRemotes();
      scheduleConfig(0);
    } catch (e) {
      if (!configCurrent(g) || config.read !== ctl || !configReadable()) return;
      config.loading = config.needsInit = false;
      body.replaceChildren(note("暂时无法读取托管模块。"), button("重试", () => navigate(tab, remote)));
      showNotice(e.message, true);
    } finally { if (config.read === ctl) config.read = null; }
  }
  async function mutate(path, data, done, b) {
    if (config.busy) return;
    const g = config.gen; config.writeEpoch++; config.busy = true; clearTimeout(config.timer);
    config.busyButton = b || null; if (b) b.disabled = true; showNotice("");
    try { const out = await configApi(path, data, g); if (configCurrent(g)) await done(out); }
    catch (e) { if (configCurrent(g)) showNotice(e.message + "；如果请求已送达，请先刷新确认结果，不要连续重复操作。", true); }
    finally { if (configCurrent(g)) { config.busy = false; config.busyButton = null; if (b?.isConnected) b.disabled = false; scheduleConfig(0); } }
  }
  function stateAfter(out) {
    if (out && "host" in out) config.status = out;
  }
  function safetyNotes() {
    const n = el("details", "cch-help"); n.append(el("summary", "", "使用前须知"),
      note("文件、终端和执行环境留在主机；这里只同步文字与会话控制，不传工作区文件。"),
      note("主机请保持供电、联网并允许后台运行。应用退到后台可继续，但系统杀进程、断网或关机后无法远程控制。"),
      note("更新应用或工具包前，请先“暂停托管连接”，以免应用为保护托管而延后安装。系统强杀或设备重启后，仍需手动打开应用恢复托管。"),
      note("配对码持有人可操作所有已托管会话，请勿公开。原生安装、Shizuku 等系统确认仍须在主机本机处理。")); return n;
  }

  function renderHost() {
    body.replaceChildren(); config.hostUI = null; const h = config.status.host;
    body.append(note("让这台设备继续执行任务，另一台设备只看文字、发送消息和处理会话授权。"));
    if (!h) {
      const form = card("建立托管主机");
      const name = input("text", "例如：家里的平板", "我的平板"); name.maxLength = 80;
      const url = input("url", "https://relay.example.invalid");
      const token = input("password", "服务器注册令牌"); token.maxLength = 200;
      const create = button("建立主机", () => {
        if (!name.value.trim() || !url.value.trim() || !token.value.trim()) return showNotice("请填写设备名、服务器地址和注册令牌。", true);
        mutate("host", { name: name.value.trim(), url: url.value.trim(), enrollToken: token.value.trim() }, async (out) => {
          token.value = ""; stateAfter(out); if (!(out && "host" in out)) config.status = await configApi("status"); renderHost();
          showNotice("主机已建立。请选择要托管的会话，再把配对码交给另一台设备。");
        }, create);
      }, "cch-primary");
      form.append(field("设备名称", name), field("中转服务器", url), field("注册令牌", token),
        note("注册令牌用于建立私有中转房间，不是 API Key；配对设备时不需要它。"), create);
      body.append(form, safetyNotes()); return;
    }
    const summary = card(h.name || "本机托管");
    const hostStatus = el("p", "cch-state");
    summary.append(note(h.url), hostStatus);
    const remove = button(h.removal === "confirmed" ? "重试清除本机记录" : h.removal ? "重试吊销配对" : "吊销所有配对", () => {
      if (h.removal !== "confirmed" && !confirm("停止本机托管并尝试在中转吊销所有配对？\n\n本机任务和文件不受影响。如果中转未确认，可以重试。")) return;
      mutate("host-remove", { room: h.removalRoom }, (out) => {
        stateAfter(out); renderHost();
        if (out.removalResult === "revoked") showNotice("中转已确认吊销所有配对，本机托管记录已清除；本机任务未停止。");
        else showNotice("当前没有本机主机记录，无法据此确认中转状态。");
      }, remove);
    }, "cch-danger");
    if (h.removal) {
      const messages = {
        working: "本机已停止接受托管请求，正在处理中转吊销。",
        pending: "本机已停用；上一次吊销没有确认结果，请重试。",
        failed: "本机已停用；中转是否已吊销尚未确认。旧版本中转在回执丢失后可能无法确认。",
        confirmed: "中转已确认吊销；本机记录尚未清除，请检查存储后重试。",
        "save-failed": "本机连接已停止，但未能保存停用状态；请在重启前检查存储并重试。",
      };
      hostStatus.textContent = messages[h.removal] || "本机正在解除托管，请刷新确认。";
      remove.disabled = h.removal === "working";
      summary.append(remove);
      if (h.removal === "failed") {
        const local = button("仅清除本机记录", () => {
          if (!confirm("仅清除本机的托管记录？\n\n这不会向中转发送吊销请求，无法确认服务器上的房间已撤销。清除后本机不能再用旧凭据重试，重新托管会建立新房间。")) return;
          mutate("host-remove-local", { room: h.removalRoom, confirm: true }, (out) => {
            stateAfter(out); renderHost(); showNotice("仅清除了本机托管记录；未确认中转房间状态。", true);
          }, local);
        }, "cch-danger");
        summary.append(note("建议优先重试吊销。若无法确认，可放弃本机记录；此操作不等于服务器已吊销。"), local);
      }
      body.append(summary, safetyNotes()); return;
    }
    const toggle = button(h.enabled ? "暂停托管连接" : "启用托管连接", () => mutate("host-toggle", { enabled: !config.status.host.enabled }, async (out) => {
      stateAfter(out); config.status = await configApi("status"); renderHost();
      showNotice(config.status.host.enabled ? "托管连接已启用。" : "托管连接已暂停，本机任务不会停止。");
    }, toggle));
    const codeBox = el("div", "cch-code"); codeBox.hidden = true;
    const pairing = button("显示配对码", () => mutate("pair-code", {}, ({ code }) => {
      codeBox.replaceChildren(); codeBox.hidden = false;
      const area = el("textarea"); area.readOnly = true; area.setAttribute("readonly", ""); area.rows = 4; area.value = code; area.setAttribute("aria-label", "敏感配对码");
      const copy = button("复制配对码", async () => {
        const g = config.gen; let ok = false;
        try { ok = !!window.CCApp?.copyText?.(area.value); } catch {}
        if (!ok) try { await navigator.clipboard.writeText(area.value); ok = true; } catch {}
        if (!ok) { area.focus(); area.select(); try { ok = document.execCommand("copy"); } catch {} }
        if (configCurrent(g)) showNotice(ok ? "已复制配对码。请只发给你信任的设备，用完后清理剪贴板。" : "自动复制未成功，请长按配对码复制。", !ok);
      });
      codeBox.append(note("这段配对码包含控制密钥，请勿截图公开或转发给他人。"), area, row(copy, button("隐藏", () => { area.value = ""; codeBox.replaceChildren(); codeBox.hidden = true; })));
    }, pairing));
    summary.append(row(toggle, pairing), codeBox, remove);
    const shares = card("托管哪些会话");
    shares.append(note("会话移入回收站或被彻底删除时会取消托管；恢复后需要手动重新开启。暂不可用的记录可在此清理。"));
    const currentId = window.__cc?.view?.source ? null : window.__cc?.view?.sessionId;
    const currentShared = arr(h.shares).some((s) => s.id === currentId);
    const shareCurrent = button(currentShared ? "当前会话已托管" : "托管当前会话", () => {
      const view = window.__cc?.view, sid = view?.source ? null : view?.sessionId;
      if (!sid) return showNotice("请先打开一条本机会话，再开启托管。", true);
      changeShare(sid, true, shareCurrent);
    }, "cch-primary");
    shareCurrent.disabled = !currentId || currentShared;
    const all = button("选择其他本地会话", async () => {
      const g = config.gen; all.disabled = true;
      try {
        const { sessions } = await configApi("local-sessions", undefined, g); if (!configCurrent(g)) return;
        const select = el("select"); select.setAttribute("aria-label", "选择本地会话");
        for (const s of arr(sessions)) { const opt = el("option", "", (s.running ? "运行中 · " : "") + s.title); opt.value = s.id; select.append(opt); }
        const enable = button("开启托管", () => { if (select.value) changeShare(select.value, true, enable); });
        enable.disabled = !arr(sessions).length;
        picker.replaceChildren(select, enable);
      } catch (e) { if (configCurrent(g)) showNotice(e.message, true); }
      finally { if (all.isConnected) all.disabled = false; }
    });
    const picker = el("div", "cch-row");
    const sharedList = el("div", "cch-list");
    shares.append(row(shareCurrent, all), picker, sharedList);
    config.hostUI = { hostStatus, sharedList, toggle, shareCurrent, signature: "" }; updateHost();
    body.append(summary, shares, safetyNotes());
  }
  function changeShare(sessionId, enabled, b) {
    mutate("share", { sessionId, enabled }, async (out) => {
      stateAfter(out); config.status = await configApi("status"); renderHost();
      showNotice(enabled ? (config.status.host?.enabled ? "已开启会话托管。" : "已保存会话授权；主机连接仍已暂停。")
        : "该会话已取消托管，远端控制权限已撤销。");
    }, b);
  }
  function updateHost() {
    const ui = config.hostUI, h = config.status?.host; if (!ui || !h) return;
    ui.hostStatus.textContent = !h.enabled ? "托管已暂停 · 本机任务照常" : h.online ? "中转连接正常 · 等待配对设备访问" : "正在连接或主机离线" + (h.error ? " · " + brief(h.error) : "");
    ui.hostStatus.classList.toggle("cch-ok", !!h.enabled && !!h.online);
    ui.toggle.textContent = h.enabled ? "暂停托管连接" : "启用托管连接";
    const currentId = window.__cc?.view?.source ? null : window.__cc?.view?.sessionId;
    const shared = arr(h.shares).some((s) => s.id === currentId);
    ui.shareCurrent.disabled = !currentId || shared;
    ui.shareCurrent.textContent = shared ? "当前会话已托管" : "托管当前会话";
    const sig = JSON.stringify(h.shares);
    if (sig === ui.signature) return; ui.signature = sig; ui.sharedList.replaceChildren();
    if (!arr(h.shares).length) ui.sharedList.append(note("还没有托管会话。其他设备目前看不到你的对话。"));
    for (const s of arr(h.shares)) {
      const n = el("div", "cch-listrow"), copy = el("div"); copy.append(el("b", "", s.title || s.id),
        note(s.revocationPending ? "撤销尚未保存 · 远端访问已阻止" : s.unavailable ? "会话暂不可用 · 此授权仍占用名额"
          : s.waiting ? `等待 ${s.waiting} 项确认` : s.running ? "正在运行" : "待命"));
      const cancel = button(s.unavailable ? "清理托管记录" : "取消托管", () => changeShare(s.id, false, cancel), "cch-quiet");
      n.append(copy, cancel); ui.sharedList.append(n);
    }
  }
  function renderRemotes() {
    body.replaceChildren(); body.append(note("连接主机后，可以操作它主动托管的会话；文件和执行仍在主机上。"));
    const list = card("已配对设备");
    if (!arr(config.status.remotes).length) list.append(note("尚未配对。在平板的“此设备作为主机”里生成配对码，再填到下面。"));
    for (const remote of arr(config.status.remotes)) {
      const line = el("div", "cch-listrow"), info = el("div"); info.append(el("b", "", remote.name), note(remote.url));
      const connect = button("连接", () => navigate("remote", remote), "cch-primary");
      const forget = button("移除", () => {
        if (!confirm(`只从本机移除“${remote.name}”的配对？主机任务不受影响。`)) return;
        mutate("forget", { remoteId: remote.id }, async () => {
          if (state.remote?.id === remote.id) closeRemotePage();
          window.__cc?.chatSource?.forget?.(remote.id);
          config.status = await configApi("status"); renderRemotes();
        }, forget);
      }, "cch-quiet");
      line.append(info, row(connect, forget)); list.append(line);
    }
    const form = card("连接新设备");
    const name = input("text", "例如：家里的平板"); name.maxLength = 80;
    const code = el("textarea"); code.rows = 4; code.placeholder = "粘贴 CCH1. 开头的配对码"; code.maxLength = 3000; code.autocomplete = "off"; code.spellcheck = false;
    const pair = button("保存配对", () => {
      if (!name.value.trim() || !code.value.trim()) return showNotice("请填写设备名称并粘贴配对码。", true);
      mutate("pair", { code: code.value.trim(), name: name.value.trim() }, async () => {
        code.value = ""; config.status = await configApi("status"); renderRemotes(); showNotice("配对配置已保存。点“连接”验证主机是否在线。");
      }, pair);
    }, "cch-primary");
    form.append(field("给主机起个名称", name), field("主机配对码", code), pair,
      note("配对码相当于已托管会话的控制权。请只导入你信任的主机，不要公开自己的配对码。"));
    body.append(list, form, safetyNotes());
  }

  async function rpc(op, args = {}, extra = {}, signal) {
    const g = state.gen, epoch = state.writeEpoch, remote = state.remote, s = state.session;
    const reading = state.readEpoch;
    if (!remote) throw new Error("请先选择主机");
    const out = await api("rpc", { remoteId: remote.id, id: id(), op, ...(s ? { sessionId: s.id } : {}),
      ...(state.remoteList ? { boot: state.remoteList.boot } : {}), ...(s ? { grant: s.grant } : {}), args, ...extra }, g, signal);
    if (!current(g)) throw new Error("界面已切换");
    if (epoch === state.writeEpoch && (!signal || reading === state.readEpoch)) {
      state.online = !document.hidden; state.seen = Date.now(); state.lastError = "";
      clearTimeout(state.watchdog);
      state.watchdog = setTimeout(() => { if (current(g)) { state.online = false; updateControls(); } }, 12000);
    }
    return out.result;
  }
  function renderRemoteSessions() {
    body.replaceChildren();
    const heading = row(button("‹ 设备", () => navigate("remote"), "cch-quiet"), el("h3", "", config.remote.name),
      button("刷新", () => scheduleConfig(0), "cch-quiet"));
    const conn = el("p", "cch-state", "连接主机中…"); conn.id = "cch-connection";
    const list = el("div", "cch-list"); list.id = "cch-remote-sessions";
    body.append(heading, conn, list, note("这里只显示主机已开启托管的会话。没有出现时，请在主机的托管入口开启。"));
  }
  function showRemoteList(data) {
    config.remoteList = data;
    const list = document.getElementById("cch-remote-sessions"); if (!list) return;
    const sig = JSON.stringify(data.sessions); if (list.dataset.signature === sig) return;
    list.dataset.signature = sig; list.replaceChildren();
    if (!arr(data.sessions).length) list.append(note("主机在线，但暂未托管任何会话。"));
    for (const s of arr(data.sessions)) {
      const remote = config.remote;
      const line = button("", () => openRemotePage(remote, s.id, s), "cch-session");
      line.append(el("b", "", s.title || s.id), el("small", "", s.waiting ? `等待 ${s.waiting} 项确认` : s.running ? "正在运行" : "待命")); list.append(line);
    }
  }
  function scheduleConfig(ms = 2500) {
    clearTimeout(config.timer);
    if (!configReadable() || config.loading || config.fetching || config.busy || !config.status) return;
    config.timer = setTimeout(pollConfig, ms);
  }
  async function pollConfig() {
    if (!configReadable() || config.loading || config.fetching || config.busy || !config.status) return;
    const g = config.gen, epoch = config.writeEpoch, ctl = config.read = new AbortController(); config.fetching = true;
    const valid = () => configCurrent(g) && config.read === ctl && configReadable() && epoch === config.writeEpoch && !config.busy;
    try {
      if (config.tab === "host") {
        const out = await configApi("status", undefined, g, ctl);
        if (!valid()) return;
        const prior = config.status.host;
        const changed = !!out.host !== !!prior || out.host?.removal !== prior?.removal || out.host?.removalRoom !== prior?.removalRoom;
        config.status = out;
        if (changed) renderHost(); else updateHost();
      } else if (config.remote) {
        const data = (await configApi("rpc", { remoteId: config.remote.id, id: id(), op: "list" }, g, ctl)).result;
        if (!valid()) return;
        config.online = true; config.error = ""; showRemoteList(data);
        const conn = document.getElementById("cch-connection");
        if (conn) { conn.textContent = "主机在线"; conn.classList.remove("cch-error"); }
      }
    } catch (e) {
      if (valid()) {
        config.online = false; config.error = brief(e.message);
        const conn = document.getElementById("cch-connection");
        if (conn) { conn.textContent = "主机暂不可达 · " + config.error; conn.classList.add("cch-error"); }
        else showNotice(e.message, true);
      }
    } finally { if (config.read === ctl) { config.read = null; config.fetching = false; scheduleConfig(config.error ? 5000 : 2500); } }
  }
  document.addEventListener("cc:source", () => { if (configReadable() && config.tab === "host") updateHost(); });
  document.addEventListener("visibilitychange", () => {
    if (!config.open) return;
    if (document.hidden) {
      config.gen++; cancelConfigRead();
      for (const c of config.ctrls) c.abort(); config.ctrls.clear();
      if (config.busyButton?.isConnected) config.busyButton.disabled = false; config.busyButton = null;
      config.fetching = false; config.busy = false;
    } else resumeConfigRead();
  });
  // 远程只提供来源适配；页面、输入、正文和历史窗口都由原聊天页管理。
  function openSession(s) { openRemotePage(state.remote, s.id, s); }
  function openRemotePage(remote, sid, known = null) {
    if (!remote) return;
    return openNativeRemote(remote, sid, known);
  }
  function saveNativeTransport() {
    if (state.native) state.native.transport = { revision: state.revision, boot: state.remoteList?.boot, grant: state.session?.grant };
  }
  function openNativeRemote(remote, sid, known = null) {
    const native = window.__cc?.chatSource;
    if (!native) return showNotice("当前主界面未提供会话来源入口，请更新应用。", true);
    if (config.open) close();
    if (state.native?.sid === sid && state.remote?.id === remote.id) {
      if (known && !state.session) { state.session = known; state.pendingOpen = ""; state.native.title = known.title || state.native.title; schedule(0); }
      window.__cc?.closeDrawer?.(); return;
    }
    saveNativeTransport(); cancelView(); cancelBackground();
    const cached = remoteIndex.get(remote.id), key = "remote:" + remote.id + ":" + sid;
    const restored = native.cached(key);
    state.pageOpen = true; state.tab = "remote"; state.remote = remote;
    state.remoteList = cached ? { boot: cached.boot, sync: cached.sync, info: cached.info, history: cached.history, sessions: cached.sessions } : null;
    state.session = known || (cached?.sessions || []).find(x => x.id === sid) || null;
    state.pendingOpen = state.session ? "" : sid;
    const source = restored || { sid, key, name: remote.name, title: state.session?.title || remote.name,
      readPage: async args => {
        if (state.native !== source || !state.confirmed) throw new Error("请先等主机同步成功");
        if ((args.before != null || args.after != null) && state.remoteList?.history !== 1) throw new Error("主机尚不支持原页面历史窗口，请升级主机");
        try { return await rpc("session", args); }
        catch (e) {
          if (state.native === source && /历史范围已失效/.test(e.message)) {
            state.writeEpoch++; state.revision = ""; state.confirmed = false; state.negotiated = false;
            failConnection(e); schedule(0);
          }
          throw e;
        }
      },
      send: (message, inject, requestId) => nativeCommand("send", { message, inject }, requestId),
      stop: runId => nativeCommand("stop", { runId }),
      message: (action, args) => nativeCommand("message", { ...args, action, revision: state.revision }),
      answer: (p, b) => nativeCommand("answer", { runId: p.runId, id: p.id, behavior: b.behavior,
        interrupt: !!b.interrupt, ...(b.message ? { message: b.message } : {}), ...(b.answers ? { answers: b.answers } : {}) }),
      close: () => {
        if (state.native !== source) return;
        saveNativeTransport(); state.native = null; state.pageOpen = false; cancelView(); state.session = null; state.remote = null;
        refreshRemoteIndex(true);
      } };
    source.name = remote.name; source.title = state.session?.title || source.title;
    state.native = source; state.resuming = !!restored?.latest; native.open(source); schedule(0); updateControls();
  }
  async function nativeCommand(op, args, requestId) {
    if (!state.online || !state.confirmed || Date.now() - state.seen >= 12000) throw new Error("请先等主机状态同步成功");
    const source = state.native, g = state.gen; state.writeEpoch++;
    try {
      const result = await rpc(op, args, requestId ? { id: requestId } : {});
      if (op === "message" && current(g) && state.native === source) { state.revision = ""; state.confirmed = false; state.resuming = false; updateControls(); }
      return result;
    }
    catch (e) { if (current(g)) { state.negotiated = false; state.confirmed = false; state.revision = ""; failConnection(e); } throw e; }
    finally { if (current(g) && state.native === source) schedule(0); }
  }
  function closeRemotePage() {
    if (!state.native) return false;
    window.__cc?.chatSource?.close(); return true;
  }
  function updateControls() {
    const fresh = !document.hidden && state.online && (!state.pageOpen || state.confirmed) && Date.now() - state.seen < 12000;
    if (state.native) {
      state.native.connected = state.online; state.native.fresh = fresh; state.native.error = state.lastError;
      window.__cc?.chatSource?.paint(state.native); return;
    }
    const conn = document.getElementById("cch-connection");
    if (conn) {
      conn.textContent = fresh ? "主机在线" : state.lastError ? "主机暂不可达 · " + brief(state.lastError) : "正在连接主机…";
      conn.classList.toggle("cch-error", !fresh && !!state.lastError);
    }
  }
  function failConnection(e) { state.online = false; state.lastError = brief(e.message); updateControls(); }
  // watch 为主机事件唤醒的增量长轮询；无变化挂起八秒，不反复拿完整快照。
  function schedule(ms) {
    clearTimeout(state.timer);
    if (!state.pageOpen || document.hidden) return;
    if (state.fetching || state.busy) return;
    const incremental = state.pageOpen && state.negotiated && state.remoteList?.sync === 1;
    const initializing = state.pageOpen && !!state.session && !state.confirmed;
    state.timer = setTimeout(poll, ms ?? (state.lastError ? 5000 : incremental || initializing ? 0 : 2500));
  }
  function patchList(base, patch) {
    if (!Array.isArray(base) || !patch || typeof patch !== "object") throw new Error("增量数据格式无效");
    if (patch.text) {
      const { index, from, append } = patch.text;
      if (!Number.isInteger(index) || index < 0 || index >= base.length || typeof append !== "string" || base[index]?.text?.length !== from) throw new Error("增量文字边界不匹配，正在重新同步");
      const out = base.slice(); out[index] = { ...base[index], text: base[index].text + append };
      if (patch.append !== undefined) { if (!Array.isArray(patch.append)) throw new Error("增量追加无效"); out.push(...patch.append); }
      return out;
    }
    if (!Number.isInteger(patch.from) || patch.from < 0 || patch.from > base.length || patch.remove !== base.length - patch.from || !Array.isArray(patch.items)) throw new Error("增量条目边界不匹配，正在重新同步");
    return base.slice(0, patch.from).concat(patch.items);
  }
  function acceptSync(frame) {
    if (frame?.sync !== 1 || typeof frame.revision !== "string" || frame.revision.length > 128) throw new Error("主机返回了无效的同步帧");
    if (frame.reset) {
      const p = frame.snapshot;
      if (p?.id !== state.session?.id || !Array.isArray(p.items) || !Array.isArray(p.live)) throw new Error("主机快照与当前会话不匹配");
      state.revision = frame.revision; return p;
    }
    const latest = state.native?.latest;
    if (!latest || !state.revision) throw new Error("增量缺少初始快照，正在重新同步");
    if (frame.unchanged) { if (frame.revision !== state.revision) throw new Error("心跳修订号不匹配"); return null; }
    if (!frame.delta || typeof frame.delta !== "object") throw new Error("同步帧缺少增量内容");
    const p = { ...latest }, d = frame.delta;
    for (const field of ["items", "live"]) if (d[field] !== undefined) p[field] = patchList(p[field], d[field]);
    for (const field of ["run", "perms", "queue", "interrupted", "info", "range", "marks", "historyToken"]) if (Object.hasOwn(d, field)) p[field] = d[field];
    state.revision = frame.revision; return p;
  }
  function acceptPage(p) {
    if (!state.native || !p || p.id !== state.session?.id) return;
    state.confirmed = true;
    window.__cc.chatSource.apply(state.native, p);
  }
  async function poll() {
    if (!state.pageOpen || document.hidden || state.fetching || state.busy) return;
    const g = state.gen, epoch = state.writeEpoch, reading = ++state.readEpoch;
    const ctl = new AbortController(); state.readCtrl = ctl; state.fetching = true; updateControls();
    const valid = () => current(g) && epoch === state.writeEpoch && reading === state.readEpoch && !ctl.signal.aborted;
    try {
      if (state.remote) {
        if (!state.session) {
          const data = await rpc("list", {}, {}, ctl.signal); if (!valid()) return;
          const want = state.pendingOpen || state.native?.sid; state.pendingOpen = ""; state.remoteList = data;
          if (want) {
            const hit = arr(data.sessions).find((s) => s.id === want);
            if (hit) openSession(hit);
            else { if (state.native) window.__cc.chatSource.invalidate(state.native); throw new Error("这条会话已取消托管，或不在主机的托管列表里。"); }
          }
        } else {
          if (!state.negotiated || state.remoteList?.sync !== 1) {
            const data = await rpc("list", {}, {}, ctl.signal); if (!valid()) return;
            const match = arr(data.sessions).find((s) => s.id === state.session.id);
            if (!match) { if (state.native) window.__cc.chatSource.invalidate(state.native); throw new Error("该会话已取消托管或不存在，请返回会话列表"); }
            if (state.remoteList?.boot !== data.boot || state.session.grant !== match.grant) {
              state.online = false; state.confirmed = false; state.revision = ""; updateControls();
            }
            const saved = state.native?.transport;
            if (state.resuming && saved && (saved.boot !== data.boot || saved.grant !== match.grant)) window.__cc.chatSource.invalidate(state.native);
            if (state.resuming && saved?.boot === data.boot && saved?.grant === match.grant && data.resume === 1) state.revision = saved.revision || "";
            else if (state.resuming) state.revision = "";
            state.remoteList = data; state.session = match; state.negotiated = true;
            if (state.native) { state.native.title = match.title || state.native.title; state.native.actions = data.actions === 1 ? 1 : 0; }
            const cached = remoteIndex.get(state.remote.id);
            if (cached) { cached.sessions = arr(data.sessions); cached.boot = data.boot; cached.sync = data.sync; cached.info = data.info; cached.history = data.history; paintSidebar(); }
          }
          if (state.remoteList?.sync === 1) {
            const frame = await rpc("watch", { revision: state.revision || undefined, ...(state.resuming && state.remoteList?.resume === 1 ? { immediate: true } : {}) }, {}, ctl.signal); if (!valid()) return;
            acceptPage(acceptSync(frame));
            if (state.resuming && state.native?.latest) state.confirmed = true;
            state.resuming = false;
          } else {
            const p = await rpc("session", {}, {}, ctl.signal); if (!valid()) return;
            acceptPage(p);
          }
        }
      }
      if (valid()) updateControls();
    } catch (e) {
      if (valid()) {
        state.negotiated = false; state.confirmed = false; state.revision = "";
        if (state.remote) failConnection(e); else showNotice(e.message, true);
      }
    } finally {
      if (current(g) && reading === state.readEpoch) {
        const changedByWrite = epoch !== state.writeEpoch;
        state.readCtrl = null; state.fetching = false; updateControls(); schedule(changedByWrite ? 0 : undefined);
      }
    }
  }
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) cancelBackground();
    if (!state.pageOpen) return;
    if (document.hidden) {
      saveNativeTransport(); clearTimeout(state.timer);
      cancelRead();
      for (const c of state.ctrls) c.abort();
      state.online = false; state.confirmed = false; updateControls();
    } else {
      state.online = false; state.confirmed = false; state.negotiated = false;
      state.resuming = !!state.native?.latest; updateControls();
      schedule(0);
    }
  });
  // ── 侧栏「远程」分组：已配对设备的托管会话直接列出，点一下进入 ──
  // 侧栏、配置、远程聊天是三个独立的读取者。关掉侧栏不影响另外两者；配对变更只作废侧栏缓存。
  const REMOTE_FOLD = "cc.cchRemoteFold";
  const remoteIndex = new Map();   // remoteId -> { remote, sessions, boot, at, error }
  const sidebar = window.CCSidebar, sideBox = document.getElementById("side-recent-remote");
  const sideReadable = () => sidebar.visible() && !state.pageOpen && !window.__cc?.view.source;
  let sideRemotes = [], sideStatusAt = 0, sideBusy = false, sideInvalid = false;
  const remoteFolded = () => { try { return localStorage.getItem(REMOTE_FOLD) === "1"; } catch { return false; } };
  async function bgApi(pathname, payload, ms = 8000) {
    const ctl = new AbortController(), epoch = backgroundEpoch;
    backgroundCtrls.add(ctl);
    const timer = setTimeout(() => ctl.abort(), ms);
    try {
      const r = await fetch("/api/hosting/" + pathname, { method: payload === undefined ? "GET" : "POST", credentials: "same-origin",
        headers: { "Content-Type": "application/json" }, ...(payload === undefined ? {} : { body: JSON.stringify(payload) }), signal: ctl.signal });
      if (ctl.signal.aborted || epoch !== backgroundEpoch) throw new DOMException("已取消后台读取", "AbortError");
      let out; try { out = await r.json(); } catch { throw new Error("托管接口未返回有效 JSON"); }
      if (ctl.signal.aborted || epoch !== backgroundEpoch) throw new DOMException("已取消后台读取", "AbortError");
      if (!r.ok) throw new Error(brief(out?.error || out?.message, 200) || `请求失败（${r.status}）`);
      return out;
    } finally { clearTimeout(timer); backgroundCtrls.delete(ctl); }
  }
  async function refreshRemoteIndex(force = false) {
    if (force) sideInvalid = true;
    if (sideBusy || !sideReadable()) return;
    force = sideInvalid;
    if (!force && Date.now() - sideStatusAt < 20000) return;
    sideInvalid = false; sideBusy = true;
    const epoch = backgroundEpoch;
    try {
      const st = await bgApi("status");
      if (epoch !== backgroundEpoch) return;
      sideRemotes = arr(st.remotes); sideStatusAt = Date.now();
      const live = new Set(sideRemotes.map((r) => r.id));
      for (const k of [...remoteIndex.keys()]) if (!live.has(k)) remoteIndex.delete(k);
      for (const r of sideRemotes) {
        if (!sideReadable()) { sideInvalid = true; break; }
        const prev = remoteIndex.get(r.id);
        if (prev && !force && Date.now() - prev.at < 30000) { prev.remote = r; continue; }
        try {
          const data = (await bgApi("rpc", { remoteId: r.id, id: id(), op: "list" }, 26000)).result || {};
          if (epoch !== backgroundEpoch) return;
          remoteIndex.set(r.id, { remote: r, sessions: arr(data.sessions), boot: data.boot, sync: data.sync, info: data.info, history: data.history, at: Date.now(), error: "" });
        } catch (e) {
          if (epoch !== backgroundEpoch || e.name === "AbortError") return;
          remoteIndex.set(r.id, { remote: r, sessions: prev?.sessions || [], boot: prev?.boot, at: Date.now(), error: brief(e.message, 120) });
        }
      }
    } catch {} finally {
      if (epoch === backgroundEpoch) {
        sideBusy = false; paintSidebar();
        if (sideInvalid && sideReadable()) refreshRemoteIndex();
      }
    }
  }
  function paintSidebar() {
    if (!sideBox || !sidebar.visible()) return;
    const fold = remoteFolded();
    sidebar.rows(sideBox, (row) => {
      if (!sideRemotes.length) return;
      const count = sideRemotes.reduce((n, r) => n + (remoteIndex.get(r.id)?.sessions.length || 0), 0);
      row("head", "h4", [fold, fold && count], (h) => {
        h.className = "sr-g cch-remote-head" + (fold ? " shut" : ""); h.tabIndex = 0;
        h.setAttribute("role", "button"); h.setAttribute("aria-expanded", String(!fold));
        h.innerHTML = `<svg viewBox="0 0 24 24"><path d="M8 10l4 4 4-4"/></svg><span>远程</span>${fold ? `<em>${count}</em>` : ""}`;
      });
      if (fold) return;
      if (!count) {
        const down = sideRemotes.some((r) => remoteIndex.get(r.id)?.error);
        row("empty", "div", [down], (n) => { n.className = "cch-rs-empty"; n.textContent = down ? "主机暂不可达，稍后自动重试" : "主机还没有托管会话"; });
      }
      for (const r of sideRemotes) {
        const entry = remoteIndex.get(r.id); if (!entry) continue;
        for (const s of entry.sessions) {
          const active = state.pageOpen && state.remote?.id === r.id && state.native?.sid === s.id;
          const dot = s.waiting ? " ask" : s.running ? "" : " idle", label = s.title || s.id;
          row(JSON.stringify([r.id, s.id]), "button", [r.name, label, dot, active, entry.error], (b) => {
            b.type = "button"; b.className = "sr-item cch-rs-item" + (active ? " on" : "");
            b.dataset.remote = r.id; b.dataset.rsid = s.id;
            b.title = `${r.name} · ${label}` + (entry.error ? `（${entry.error}）` : "");
            b.replaceChildren(el("i", "sr-dot" + dot), el("span", "", label), el("em", "cch-rs-dev", r.name));
          });
        }
      }
    });
  }
  sideBox.classList.add("cch-remote-group");
  sideBox.addEventListener("click", (ev) => {
    if (ev.target.closest(".cch-remote-head")) {
      ev.preventDefault(); ev.stopPropagation();
      try { localStorage.setItem(REMOTE_FOLD, remoteFolded() ? "0" : "1"); } catch {}
      paintSidebar(); return;
    }
    const rowEl = ev.target.closest("[data-rsid]"); if (!rowEl) return;
    ev.preventDefault(); ev.stopPropagation();
    const remote = sideRemotes.find((r) => r.id === rowEl.dataset.remote);
    if (remote) openRemote(remote, rowEl.dataset.rsid);
  });
  function openRemote(remote, sid) { openRemotePage(remote, sid); }
  document.getElementById("drawer")?.addEventListener("click", (e) => {
    if (!state.pageOpen || e.target.closest(".cch-remote-group")) return;
    if (e.target.closest("[data-page],#side-acct")) closeRemotePage();
  }, true);
  function syncSidebar() {
    paintSidebar();
    if (!sideReadable()) { if (sideBusy) cancelBackground(); }
    else refreshRemoteIndex();
  }
  document.addEventListener("cc:sidebar", syncSidebar);
  document.addEventListener("cc:source", syncSidebar);
  syncSidebar();
  setInterval(refreshRemoteIndex, 30000);    // 索引仍按原频率取；未显示（含强刷）不发请求，不定时画列表。

  // 只供本模块的 DOM 回归测试与外部入口使用，不暴露配对凭据或主机文件接口。
  window.CCHosting = Object.freeze({ open, close, openRemote });
})();
