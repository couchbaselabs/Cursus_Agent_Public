/*
 * <cursus-assistant> — the embedded assistant panel, carved out as a
 * self-contained Web Component (Shadow DOM, no framework).
 *
 * This is the "middle path" from the NiceGUI-vs-SPA discussion: the one
 * surface NiceGUI is worst at (token streaming, tool-call traces, inline
 * charts, typewriter) lives here, isolated behind a custom element that
 * any shell — NiceGUI, React, plain HTML — can drop in.
 *
 * Boundary contract:
 *   HOST → COMPONENT
 *     attribute  customer="American Express"   global scope (single source of truth)
 *     property   .endpoint = async(payload)=>string   swap in the real streaming backend
 *     method     el.ask("Explain ticket 48213")  imperative hand-off (KPI cards, rows)
 *   COMPONENT → HOST  (CustomEvents, bubbles+composed)
 *     assistant:send    {text, customer}
 *     assistant:report  {customer}      a report was filed → host navigates/toasts
 *     assistant:toggle  {collapsed}
 *
 * With no .endpoint set, it uses window.claude.complete when present, else a
 * canned offline agent, so the prototype runs anywhere.
 */
(function () {
  const FONTS = 'https://fonts.googleapis.com/css2?family=Archivo:wght@500;600;700;800&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap';
  const WIDTH_KEY = 'cursus-assistant.width';
  const W_MIN = 300, W_MAX = 560;
  function storedWidth() {
    try { const n = parseInt(localStorage.getItem(WIDTH_KEY), 10); return Number.isFinite(n) ? Math.max(W_MIN, Math.min(W_MAX, n)) : null; } catch (e) { return null; }
  }
  function ensureFonts() {
    if (document.getElementById('cursus-assistant-fonts')) return;
    const l = document.createElement('link');
    l.id = 'cursus-assistant-fonts'; l.rel = 'stylesheet'; l.href = FONTS;
    document.head.appendChild(l);
  }

  const DATA = {
    'American Express': { health: 74, openP1: 7, nodes: 9, version: '7.6.2', atRisk: 4, sla: 1 },
    'Vodafone':         { health: 61, openP1: 5, nodes: 6, version: '7.2.4', atRisk: 3, sla: 2 },
    'Western Union':    { health: 88, openP1: 1, nodes: 4, version: '7.6.0', atRisk: 0, sla: 0 },
    'Barclays':         { health: 70, openP1: 3, nodes: 7, version: '7.6.2', atRisk: 2, sla: 0 },
  };
  const TICKETS = [
    { id: 48213, cust: 'American Express', subject: 'Indexer OOM after 7.6.2 upgrade', priority: 'P1', status: 'Open', version: '7.6.2', node: 'cbse-4', score: 86 },
    { id: 48219, cust: 'American Express', subject: 'XDCR lag on prod cluster', priority: 'P1', status: 'Open', version: '7.6.2', node: 'cbse-4', score: 78 },
    { id: 48240, cust: 'American Express', subject: 'Rebalance stuck at 61%', priority: 'P1', status: 'Open', version: '7.6.2', node: 'cbse-4', score: 83 },
    { id: 47110, cust: 'Vodafone', subject: 'Eventing function crash loop', priority: 'P1', status: 'Open', version: '7.2.4', node: 'vf-2', score: 80 },
    { id: 46020, cust: 'Western Union', subject: 'Certificate rotation failed', priority: 'P1', status: 'Open', version: '7.6.0', node: 'wu-2', score: 71 },
    { id: 45318, cust: 'Barclays', subject: 'Auto-failover misfire', priority: 'P1', status: 'Open', version: '7.6.2', node: 'bc-1', score: 74 },
  ];
  const STARTERS = [
    { label: 'Morning briefing', q: 'Give me a morning briefing across all accounts' },
    { label: "What's new?", q: "What's changed recently for this account?" },
    { label: 'Critical issues', q: 'Call query_tickets with status=open and priority=urgent. Return the full tool result table without reformatting. Group rows under bold ### Organization headers.' },
    { label: 'Generate report', q: 'Generate a health report' },
  ];

  const esc = (s) => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  // Markdown → HTML for agent bubbles. Uses marked.js when available (loaded by the host
  // page), falls back to a minimal inline renderer so the component works standalone.
  function md(s) {
    if (!s) return '';
    if (window.marked) {
      // marked v9+ uses marked.use(); older versions use setOptions()
      try {
        if (typeof window.marked.use === 'function') {
          window.marked.use({ gfm: true, breaks: true });
        } else {
          window.marked.setOptions({ gfm: true, breaks: true });
        }
      } catch (e) {}
      return window.marked.parse(s);
    }
    // Fallback: covers the most common patterns
    let t = esc(s);
    t = t.replace(/^###\s+(.+)$/gm, '<h3>$1</h3>');
    t = t.replace(/^##\s+(.+)$/gm, '<h2>$1</h2>');
    t = t.replace(/^#\s+(.+)$/gm, '<h1>$1</h1>');
    t = t.replace(/\*\*\*(.+?)\*\*\*/g, '<strong><em>$1</em></strong>');
    t = t.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    t = t.replace(/\*(.+?)\*/g, '<em>$1</em>');
    t = t.replace(/`([^`]+)`/g, '<code>$1</code>');
    t = t.replace(/^[*-]\s+(.+)$/gm, '<li>$1</li>');
    t = t.replace(/(<li>[\s\S]*?<\/li>)/g, '<ul>$1</ul>');
    t = t.replace(/^\d+\.\s+(.+)$/gm, '<li>$1</li>');
    t = t.split(/\n{2,}/).map((b) => {
      b = b.trim();
      if (!b) return '';
      if (/^<(h[1-6]|ul|ol|li|pre|blockquote)/.test(b)) return b;
      return '<p>' + b.replace(/\n/g, '<br>') + '</p>';
    }).join('\n');
    return t;
  }

  const SHELL_CSS = `
    :host{ all:initial; display:block; height:100%; width:352px; box-sizing:border-box;
      font-family:'IBM Plex Sans',system-ui,sans-serif; color:#1b1d21;
      transition:width .18s ease; contain:layout style; }
    :host([collapsed]){ width:44px; }
    *{ box-sizing:border-box; }
    .wrap{ position:relative; height:100%; display:flex; flex-direction:column; background:#fff; border-left:1px solid #ddd8ce; }
    .grip{ position:absolute; left:-3px; top:0; width:7px; height:100%; cursor:col-resize; z-index:6; touch-action:none; }
    .grip::after{ content:''; position:absolute; left:2px; top:0; width:2px; height:100%; background:transparent; transition:background .12s; }
    .grip:hover::after,.grip.active::after{ background:#ea2328; }
    .hd{ padding:12px 15px; border-bottom:1px solid #eee9df; display:flex; align-items:center; gap:9px; flex:none; }
    .dot{ width:8px; height:8px; border-radius:50%; background:#ea2328; flex:none; }
    .ttl{ font-family:'Archivo'; font-weight:700; font-size:14px; }
    .scope{ font-size:11px; color:#6b6f76; font-family:'IBM Plex Mono'; background:#f4f2ec; padding:2px 8px; border-radius:10px; white-space:nowrap; }
    .chev{ margin-left:auto; background:none; border:none; color:#9a9ea6; cursor:pointer; font-size:16px; padding:2px 4px; line-height:1; }
    .chev:hover{ color:#1b1d21; }
    .body{ flex:1; overflow:auto; padding:14px; }
    .chips{ display:flex; gap:7px; flex-wrap:wrap; margin-bottom:14px; }
    .chip{ background:#f4f2ec; border:1px solid #e4dfd4; border-radius:20px; padding:5px 11px;
      font:inherit; font-size:11.5px; color:#3c4046; cursor:pointer; transition:.12s; }
    .chip:hover{ border-color:#ea2328; color:#ea2328; }
    .row{ display:flex; margin-bottom:10px; }
    .row.user{ justify-content:flex-start; }
    .row.agent{ justify-content:flex-end; }
    .bub{ max-width:88%; padding:10px 12px; font-size:13px; line-height:1.5; animation:cuFade .22s ease; }
    .bub.user{ background:#f4f2ec; border-radius:11px 11px 11px 3px; }
    .bub.agent{ background:#fff; border:1px solid #eee9df; border-radius:11px 11px 3px 11px; }
    .bub.agent p{ margin:0 0 6px; } .bub.agent p:last-child{ margin-bottom:0; }
    .bub.agent h1,.bub.agent h2,.bub.agent h3{ margin:8px 0 4px; font-weight:600; }
    .bub.agent h3{ font-size:13px; } .bub.agent h2{ font-size:14px; }
    .bub.agent ul,.bub.agent ol{ margin:4px 0 6px; padding-left:18px; }
    .bub.agent li{ margin-bottom:2px; }
    .bub.agent code{ font-family:'IBM Plex Mono',monospace; font-size:11px; background:#f4f2ec; padding:1px 4px; border-radius:3px; }
    .bub.agent strong{ font-weight:600; }
    .bub.agent table{ border-collapse:collapse; width:100%; font-size:11.5px; margin:6px 0; }
    .bub.agent th{ background:#f4f2ec; font-weight:600; text-align:left; padding:5px 8px; border-bottom:2px solid #eee9df; white-space:nowrap; }
    .bub.agent td{ padding:4px 8px; border-bottom:1px solid #f0ece6; vertical-align:top; }
    .bub.agent tr:last-child td{ border-bottom:none; }
    .bub.agent tr:hover td{ background:#faf8f5; }
    .trace{ font-family:'IBM Plex Mono'; font-size:10px; color:#ea2328; margin-bottom:5px; }
    .caret{ display:inline-block; width:7px; height:14px; background:#ea2328; margin-left:2px; vertical-align:-2px; animation:cuBlink 1s infinite; }
    .chart{ margin-top:9px; height:74px; border-radius:7px; background:linear-gradient(180deg,#fdecec,#fff); border:1px solid #f3d3d1; display:flex; align-items:flex-end; gap:5px; padding:9px; }
    .chart i{ flex:1; border-radius:3px 3px 0 0; }
    .act{ margin-top:9px; background:none; border:1px solid #ea2328; color:#ea2328; border-radius:20px;
      padding:5px 12px; font:inherit; font-size:11.5px; cursor:pointer; font-weight:600; white-space:nowrap; transition:.12s; }
    .act:hover{ background:#ea2328; color:#fff; }
    .typing{ display:flex; gap:5px; padding:6px 2px; }
    .typing span{ width:7px; height:7px; border-radius:50%; background:#ea2328; animation:cuBlink 1s infinite; }
    .typing span:nth-child(2){ animation-delay:.2s; }
    .typing span:nth-child(3){ animation-delay:.4s; }
    .composer{ padding:12px 14px; border-top:1px solid #eee9df; flex:none; }
    .pill{ display:flex; align-items:center; gap:8px; background:#f4f2ec; border:1px solid #e4dfd4; border-radius:22px; padding:5px 6px 5px 14px; }
    .pill input{ flex:1; background:none; border:none; outline:none; font:inherit; font-size:13px; color:#1b1d21; }
    .send{ width:30px; height:30px; border-radius:50%; background:#ea2328; border:none; color:#fff; cursor:pointer; font-size:14px; flex:none; transition:.12s; }
    .send:hover{ background:#c9201d; }
    .send.stop{ background:#1b1d21; }
    .send.stop:hover{ background:#000; }
    .rail{ width:44px; height:100%; background:#fff; border:none; border-left:1px solid #ddd8ce; cursor:pointer;
      display:flex; flex-direction:column; align-items:center; gap:10px; padding-top:16px; color:#ea2328; }
    .rail:hover{ background:#faf8f4; }
    .rail .lbl{ writing-mode:vertical-rl; font-family:'Archivo'; font-weight:600; font-size:12px; letter-spacing:.04em; }
    @keyframes cuBlink{ 0%,100%{opacity:.25} 50%{opacity:1} }
    @keyframes cuFade{ from{opacity:0; transform:translateY(4px)} to{opacity:1; transform:none} }
  `;

  class CursusAssistant extends HTMLElement {
    static get observedAttributes() { return ['customer', 'collapsed']; }

    constructor() {
      super();
      this.attachShadow({ mode: 'open' });
      this.endpoint = null; // host may set an async(payload)->string streaming backend
      this._stream = null;
      this._turn = 0;
      this._busy = false;
      this._reportFiled = false;
      this._messages = [];
      this._thinking = false;
    }

    get customer() { return this.getAttribute('customer') || ''; }
    set customer(v) { if (v == null) this.removeAttribute('customer'); else this.setAttribute('customer', v); }

    connectedCallback() {
      ensureFonts();
      this._messages = [{
        role: 'agent', text: 'Hello — I\'m your Couchbase customer analyst. Select a customer name or ask me anything about your accounts.'
      }];
      this._renderShell();
      this._applyWidth();
    }

    _applyWidth() {
      const w = storedWidth();
      if (w && !this.hasAttribute('collapsed')) this.style.width = w + 'px';
    }

    attributeChangedCallback(name) {
      if (!this.shadowRoot.querySelector('.wrap') && !this.shadowRoot.querySelector('.rail')) return;
      if (name === 'collapsed') this._renderShell();
      if (name === 'customer') this._syncScope();
    }

    // ── host API ──────────────────────────────────────────────
    ask(text) {
      if (this.hasAttribute('collapsed')) this._setCollapsed(false);
      this._run(String(text || '').trim());
    }
    toggle() { this._setCollapsed(!this.hasAttribute('collapsed')); }

    _setCollapsed(v) {
      if (v) { this.setAttribute('collapsed', ''); this.style.width = ''; }
      else { this.removeAttribute('collapsed'); this._applyWidth(); }
      this._emit('assistant:toggle', { collapsed: v });
    }
    _emit(type, detail) { this.dispatchEvent(new CustomEvent(type, { detail, bubbles: true, composed: true })); }

    // ── shell render (once per collapse state) ────────────────
    _renderShell() {
      const r = this.shadowRoot;
      if (this.hasAttribute('collapsed')) {
        r.innerHTML = `<style>${SHELL_CSS}</style>
          <button class="rail" title="Expand assistant">
            <span class="dot"></span><span class="lbl">Assistant ⟨</span>
          </button>`;
        r.querySelector('.rail').onclick = () => this._setCollapsed(false);
        return;
      }
      r.innerHTML = `<style>${SHELL_CSS}</style>
        <div class="wrap">
          <div class="grip" title="Drag to resize"></div>
          <div class="hd">
            <span class="dot"></span><span class="ttl">Assistant</span>
            <span class="scope" id="scope"></span>
            <button class="chev" title="Collapse">⟩</button>
          </div>
          <div class="body">
            <div class="chips">${STARTERS.map((s, i) => `<button class="chip" data-i="${i}">${esc(s.label)}</button>`).join('')}</div>
            <div class="list"></div>
            <div class="typing" style="display:none"><span></span><span></span><span></span></div>
          </div>
          <div class="composer">
            <div class="pill">
              <input type="text" placeholder="Ask about ${esc(this.customer)}…"/>
              <button class="send" title="Send">↑</button>
            </div>
          </div>
        </div>`;
      r.querySelector('.chev').onclick = () => this._setCollapsed(true);
      r.querySelectorAll('.chip').forEach((c) => { c.onclick = () => this._run(STARTERS[+c.dataset.i].q); });
      const input = r.querySelector('.pill input');
      this._send = () => { const v = input.value.trim(); if (v && !this._busy) { input.value = ''; this._run(v); } };
      input.onkeydown = (e) => { if (e.key === 'Enter') { if (this._busy) this._stop(); else this._send(); } };
      this._wireGrip(r.querySelector('.grip'));
      this._setBusy(this._busy);
      this._syncScope();
      this._paintList();
    }

    // ── stop generation ──────────────────────────────────────
    _setBusy(v) {
      this._busy = v;
      const btn = this.shadowRoot.querySelector('.send');
      if (!btn) return;
      btn.textContent = v ? '■' : '↑';
      btn.title = v ? 'Stop generating' : 'Send';
      btn.classList.toggle('stop', v);
      btn.onclick = v ? () => this._stop() : this._send;
    }
    _stop() {
      this._turn++;                 // invalidate any in-flight async reply
      clearTimeout(this._stream);
      const last = this._messages[this._messages.length - 1];
      if (last && last.streaming) { last.streaming = false; last.action = null; } // freeze partial output
      this._setThinking(false);
      this._setBusy(false);
      this._paintList();
    }

    // ── edge-drag resize ─────────────────────────────────────
    _wireGrip(grip) {
      if (!grip) return;
      const MIN = W_MIN, MAX = W_MAX;
      grip.onpointerdown = (e) => {
        e.preventDefault();
        const startX = e.clientX;
        const startW = this.getBoundingClientRect().width;
        grip.classList.add('active');
        grip.setPointerCapture(e.pointerId);
        const move = (ev) => {
          const w = Math.max(MIN, Math.min(MAX, startW + (startX - ev.clientX)));
          this.style.width = w + 'px';
        };
        const up = (ev) => {
          grip.classList.remove('active');
          grip.releasePointerCapture(e.pointerId);
          grip.removeEventListener('pointermove', move);
          grip.removeEventListener('pointerup', up);
          try { localStorage.setItem(WIDTH_KEY, String(Math.round(this.getBoundingClientRect().width))); } catch (err) {}
        };
        grip.addEventListener('pointermove', move);
        grip.addEventListener('pointerup', up);
      };
    }

    _syncScope() {
      const r = this.shadowRoot;
      const cust = this.customer;
      const scope = r.querySelector('#scope');
      if (scope) scope.textContent = cust ? 'scope: ' + cust.split(' ')[0] : 'all accounts';
      const input = r.querySelector('.pill input');
      if (input) input.placeholder = cust ? 'Ask about ' + cust + '…' : 'Ask about your accounts…';
    }

    _paintList() {
      const list = this.shadowRoot.querySelector('.list');
      if (!list) return;
      list.innerHTML = this._messages.map((m) => {
        const bar = [55, 80, 40, 66].map((h, i) => `<i style="height:${h}%;background:${i < 2 ? '#ea2328' : '#f28b82'}"></i>`).join('');
        const inner = [
          m.role === 'agent' && m.tool ? `<div class="trace">▸ ${esc(m.tool)}</div>` : '',
          m.role === 'agent' ? (m.html || md(m.text)) : esc(m.text),
          m.streaming ? '<span class="caret"></span>' : '',
          m.chart ? `<div class="chart">${bar}</div>` : '',
          m.action ? `<button class="act">${esc(m.action)}</button>` : '',
        ].join('');
        return `<div class="row ${m.role}"><div class="bub ${m.role}">${inner}</div></div>`;
      }).join('');
      // wire action buttons
      const rows = list.querySelectorAll('.row.agent');
      this._messages.forEach((m, idx) => {
        if (!m.action) return;
        const btn = list.querySelectorAll('.act')[[...this._messages.slice(0, idx)].filter((x) => x.action).length];
      });
      list.querySelectorAll('.act').forEach((btn) => { btn.onclick = () => this._fileReport(); });
      const body = this.shadowRoot.querySelector('.body');
      if (body) body.scrollTop = body.scrollHeight;
    }

    _setThinking(v) {
      this._thinking = v;
      const t = this.shadowRoot.querySelector('.typing');
      if (t) t.style.display = v ? 'flex' : 'none';
      const body = this.shadowRoot.querySelector('.body');
      if (v && body) body.scrollTop = body.scrollHeight;
    }

    // ── agent turn ────────────────────────────────────────────
    async _run(text) {
      if (!text) return;
      this._messages.push({ role: 'user', text });
      this._paintList();
      this._emit('assistant:send', { text, customer: this.customer });
      const turn = ++this._turn;
      this._setBusy(true);
      this._setThinking(true);

      // 1) host-supplied streaming endpoint
      if (typeof this.endpoint === 'function') {
        try {
          const out = await this.endpoint({ text, customer: this.customer, history: this._messages });
          if (turn !== this._turn) return;
          this._setThinking(false);
          return this._streamIn(typeof out === 'string' ? { text: out } : out, turn);
        } catch (e) { if (turn !== this._turn) return; this._setThinking(false); }
      }

      // 2) live Claude agent (in-host) with the three grounding tools
      if (window.claude && window.claude.complete) {
        try { return await this._claudeTurn(text, turn); } catch (e) { if (turn !== this._turn) return; this._setThinking(false); }
      }

      // 3) offline canned fallback
      setTimeout(() => { if (turn !== this._turn) return; this._setThinking(false); this._streamIn(this._canned(text), turn); }, 480);
    }

    async _claudeTurn(text, turn) {
      const cust = this.customer, c = DATA[cust];
      const tix = TICKETS.filter((t) => t.cust === cust);
      const used = [];
      const tools = [
        { name: 'query_tickets', description: 'List support tickets for the current customer, optionally filtered by priority (P1/P2/P3) and status (Open/Pending/Solved).',
          input_schema: { type: 'object', properties: { priority: { type: 'string' }, status: { type: 'string' } } },
          run: async (i = {}) => { used.push('query_tickets'); let r = tix; if (i.priority) r = r.filter((t) => t.priority === i.priority); if (i.status) r = r.filter((t) => t.status === i.status); return JSON.stringify(r); } },
        { name: 'get_customer_health_score', description: 'Health score and fleet stats for the current customer.',
          input_schema: { type: 'object', properties: {} },
          run: async () => { used.push('get_customer_health_score'); return JSON.stringify({ customer: cust, ...c }); } },
        { name: 'generate_customer_report', description: 'Generate a customer-ready report and file it to Reports & Automation. Call when the user asks for a report.',
          input_schema: { type: 'object', properties: {} },
          run: async () => { used.push('generate_customer_report'); this._fileReport(true); return JSON.stringify({ status: 'filed', customer: cust }); } },
      ];
      const history = this._messages.filter((m) => m.text && !m.streaming)
        .map((m) => ({ role: m.role === 'agent' ? 'assistant' : 'user', content: m.text }));
      const system = `You are the embedded assistant inside Cursus, a Couchbase support console. The active customer is "${cust}". Always ground answers in the tools before responding; never invent ticket IDs or numbers. Reply in 2–4 concise sentences for a support/AE audience. Offer to generate a customer report when it would help.`;
      const out = await window.claude.complete({ model: 'claude-sonnet-4-5', max_tokens: 600, system, messages: history, tools });
      if (turn !== this._turn) return;
      this._setThinking(false);
      const offer = /\breport\b/i.test(out) && !used.includes('generate_customer_report');
      this._streamIn({ tool: used.length ? [...new Set(used)].join(' · ') : 'agent_route', text: (out || '').trim(), action: offer ? 'Generate report' : null }, turn);
    }

    _canned(text) {
      const cust = this.customer, c = DATA[cust], q = text.toLowerCase();
      if (q.includes('brief')) return { tool: 'query_tickets · rank_portfolio', text: `Morning briefing for ${cust}: ${c.openP1} open P1s, health ${c.health}, ${c.atRisk} at-risk clusters. The indexer OOM on cbse-4 is the top regression to clear today.` };
      if (q.includes('new')) return { tool: 'get_recent_changes', text: `Last 24h for ${cust}: 2 new P1s filed, 1 cluster upgraded to ${c.version}, and XDCR lag cleared on cbse-2.` };
      if (q.includes('p1')) return { tool: 'query_tickets', text: `${c.openP1} open P1 tickets for ${cust}. Top: indexer OOM after upgrade (48213), XDCR lag on prod (48219).`, chart: true };
      if (q.includes('report')) return { tool: 'generate_health_report', text: `I can generate a full HTML health report for ${cust} covering health score, open tickets, SLA compliance and cluster topology.`, action: 'Generate report' };
      if (q.includes('health') || q.includes('why')) return { tool: 'get_customer_health_score', text: `${cust} health is ${c.health}. ${c.openP1} open P1s and ${c.atRisk} at-risk clusters are the main drags — XDCR lag on cbse-4 leads.` };
      return { tool: 'agent_route', text: `Looking at ${cust} (health ${c.health}, ${c.openP1} open P1s). Ask me for a briefing, what's new, open P1s, or a customer report.` };
    }

    _fileReport(silent) {
      // strip the offer action from prior bubbles
      this._messages.forEach((m) => { if (m.action) m.action = null; });
      this._paintList();
      // Route through the real agent so generate_health_report tool is called
      const cust = this.customer;
      const prompt = cust
        ? `Generate a health report for ${cust}`
        : 'Generate a health report';
      if (!silent) this._run(prompt);
    }

    _streamIn(msg, turn) {
      if (turn !== undefined && turn !== this._turn) return;
      const m = { role: 'agent', tool: msg.tool || null, text: '', html: null, action: null, chart: false, streaming: true };
      this._messages.push(m);
      this._paintList();
      const full = msg.text || '';
      const finalHtml = msg.html || null; // server-rendered HTML, used after animation
      const step = Math.max(2, Math.round(full.length / 55));
      let i = 0;
      clearTimeout(this._stream);
      const tick = () => {
        if (turn !== undefined && turn !== this._turn) return;
        i += step;
        if (i < full.length) { m.text = full.slice(0, i); this._paintList(); this._stream = setTimeout(tick, 26); }
        else {
          // Animation done — swap in the server-rendered HTML if available
          m.text = full;
          m.html = finalHtml;
          m.streaming = false;
          m.action = msg.action || null;
          m.chart = !!msg.chart;
          this._setBusy(false);
          this._paintList();
        }
      };
      tick();
    }
  }

  if (!customElements.get('cursus-assistant')) customElements.define('cursus-assistant', CursusAssistant);
})();
