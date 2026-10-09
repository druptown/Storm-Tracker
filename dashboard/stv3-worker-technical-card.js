class Stv3WorkerTechnicalCard extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._config = null;
    this._hass = null;
    this._mounted = false;
  }

  setConfig(config) {
    if (!config || typeof config.entity !== "string" || !config.entity.trim()) {
      throw new Error("Een expliciete worker technical diagnostics-entity is verplicht");
    }
    this._config = { entity: config.entity.trim() };
    this._mount();
    this._render();
  }

  set hass(hass) {
    this._hass = hass;
    this._mount();
    this._render();
  }

  getCardSize() {
    return 8;
  }

  _mount() {
    if (this._mounted) {
      return;
    }
    this._mounted = true;
    this.shadowRoot.innerHTML = `
      <style>
        :host {
          display: block;
        }
        ha-card {
          overflow: hidden;
        }
        .header {
          align-items: center;
          display: flex;
          gap: 12px;
          justify-content: space-between;
          padding: 18px 20px 10px;
        }
        .title {
          font-size: 1.25rem;
          font-weight: 600;
        }
        .badge {
          background: var(--secondary-background-color);
          border-radius: 999px;
          font-size: 0.78rem;
          font-weight: 700;
          padding: 5px 10px;
          text-transform: uppercase;
        }
        .badge[data-status="current"] {
          color: var(--success-color, #2e7d32);
        }
        .badge[data-status="stale"] {
          color: var(--warning-color, #ed6c02);
        }
        .badge[data-status="unknown"] {
          color: var(--secondary-text-color);
        }
        .message {
          color: var(--secondary-text-color);
          padding: 8px 20px 20px;
        }
        .grid {
          display: grid;
          gap: 12px;
          grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
          padding: 8px 16px 16px;
        }
        section {
          border: 1px solid var(--divider-color);
          border-radius: 12px;
          min-width: 0;
          padding: 12px;
        }
        h3 {
          font-size: 0.9rem;
          margin: 0 0 8px;
        }
        dl {
          display: grid;
          gap: 5px 10px;
          grid-template-columns: minmax(90px, auto) minmax(0, 1fr);
          margin: 0;
        }
        dt {
          color: var(--secondary-text-color);
          font-size: 0.78rem;
        }
        dd {
          font-family: var(--code-font-family, monospace);
          font-size: 0.78rem;
          margin: 0;
          overflow-wrap: anywhere;
          text-align: right;
        }
        .notice {
          background: var(--secondary-background-color);
          color: var(--secondary-text-color);
          font-size: 0.76rem;
          line-height: 1.4;
          padding: 10px 20px;
        }
        [hidden] {
          display: none !important;
        }
      </style>
      <ha-card>
        <div class="header">
          <div class="title">Worker technische diagnose</div>
          <span class="badge" id="overall-status" data-status="unknown">unknown</span>
        </div>
        <div class="message" id="message">Wachten op een diagnose-entity.</div>
        <div class="grid" id="content" hidden>
          <section>
            <h3>Worker en configuratie</h3>
            <dl>
              <dt>Status</dt><dd id="worker-state">unknown</dd>
              <dt>Heartbeatleeftijd</dt><dd id="worker-age">unknown</dd>
              <dt>Workerversie</dt><dd id="worker-version">unknown</dd>
              <dt>Build</dt><dd id="build-hash">unknown</dd>
              <dt>Configuratie</dt><dd id="config-hash">unknown</dd>
              <dt>Desired state</dt><dd id="desired-hash">unknown</dd>
            </dl>
          </section>
          <section>
            <h3>Versheid</h3>
            <dl>
              <dt>Provider</dt><dd id="provider-freshness">unknown</dd>
              <dt>Frame</dt><dd id="frame-freshness">unknown</dd>
              <dt>Cache</dt><dd id="cache-freshness">unknown</dd>
            </dl>
          </section>
          <section>
            <h3>Jobs</h3>
            <dl>
              <dt>Groepsstatus</dt><dd id="jobs-status">unknown</dd>
              <dt>Wachtrij</dt><dd id="queue-depth">unknown</dd>
              <dt>Afgerond</dt><dd id="terminal-jobs">unknown</dd>
              <dt>Mislukt</dt><dd id="failed-jobs">unknown</dd>
              <dt>Begrensd</dt><dd id="counts-saturated">unknown</dd>
            </dl>
          </section>
          <section>
            <h3>Retentie en capaciteit</h3>
            <dl>
              <dt>Groepsstatus</dt><dd id="storage-status">unknown</dd>
              <dt>Retentiealarm</dt><dd id="retention-alarm">unknown</dd>
              <dt>Capaciteitsalarm</dt><dd id="capacity-alarm">unknown</dd>
              <dt>Vrije ruimte</dt><dd id="headroom-bytes">unknown</dd>
              <dt>Vrij percentage</dt><dd id="headroom-percent">unknown</dd>
              <dt>Redencodes</dt><dd id="storage-reasons">unknown</dd>
            </dl>
          </section>
          <section>
            <h3>Current versus next</h3>
            <dl>
              <dt>Groepsstatus</dt><dd id="transition-status">unknown</dd>
              <dt>Fase</dt><dd id="transition-phase">unknown</dd>
              <dt>Current</dt><dd id="current-version">unknown</dd>
              <dt>Next</dt><dd id="next-version">unknown</dd>
              <dt>Redencodes</dt><dd id="transition-reasons">unknown</dd>
            </dl>
          </section>
          <section>
            <h3>Replay</h3>
            <dl>
              <dt>Groepsstatus</dt><dd id="replay-status">unknown</dd>
              <dt>Contractversie</dt><dd id="replay-contract">unknown</dd>
              <dt>Algoritmeversie</dt><dd id="replay-algorithm">unknown</dd>
              <dt>Projectieredenen</dt><dd id="projection-reasons">unknown</dd>
            </dl>
          </section>
        </div>
        <div class="notice">
          Alleen technische control-plane-diagnose. Dit is geen meteorologische
          waarheid, operationele toelating of waarschuwing.
        </div>
      </ha-card>
    `;
  }

  _render() {
    if (!this._mounted || !this._config || !this._hass) {
      return;
    }
    const entity = this._hass.states?.[this._config.entity];
    const message = this.shadowRoot.getElementById("message");
    const content = this.shadowRoot.getElementById("content");
    if (!entity) {
      message.hidden = false;
      message.textContent = "De geconfigureerde diagnose-entity bestaat niet.";
      content.hidden = true;
      this._setStatus("unknown");
      return;
    }
    const attributes = this._object(entity.attributes);
    if (attributes.contract !== "stv3-ha-worker-technical-diagnostics-v1") {
      message.hidden = false;
      message.textContent = "De diagnose-entity heeft geen ondersteund contract.";
      content.hidden = true;
      this._setStatus("unknown");
      return;
    }

    message.hidden = true;
    content.hidden = false;
    const worker = this._object(attributes.worker);
    const freshness = this._object(attributes.freshness);
    const jobs = this._object(attributes.jobs);
    const storage = this._object(attributes.storage);
    const transition = this._object(attributes.transition);
    const replay = this._object(attributes.replay);

    this._setStatus(this._status(attributes.status));
    this._setText("worker-state", this._workerState(worker.state));
    this._setText("worker-age", this._age(worker.age_seconds));
    this._setText("worker-version", this._version(worker.version));
    this._setText("build-hash", this._hash(worker.build_sha256));
    this._setText("config-hash", this._hash(worker.config_sha256));
    this._setText("desired-hash", this._hash(worker.desired_state_sha256));

    this._setText(
      "provider-freshness",
      this._freshness(this._object(freshness.provider)),
    );
    this._setText(
      "frame-freshness",
      this._freshness(this._object(freshness.frame)),
    );
    this._setText(
      "cache-freshness",
      this._freshness(this._object(freshness.cache)),
    );

    this._setText("jobs-status", this._status(jobs.status));
    this._setText("queue-depth", this._counter(jobs.queue_depth));
    this._setText("terminal-jobs", this._counter(jobs.terminal_jobs));
    this._setText("failed-jobs", this._counter(jobs.failed_jobs));
    this._setText(
      "counts-saturated",
      typeof jobs.counts_saturated === "boolean"
        ? String(jobs.counts_saturated)
        : "unknown",
    );

    this._setText("storage-status", this._status(storage.status));
    this._setText("retention-alarm", this._alarm(storage.retention_alarm));
    this._setText("capacity-alarm", this._alarm(storage.capacity_alarm));
    this._setText("headroom-bytes", this._bytes(storage.headroom_bytes));
    this._setText(
      "headroom-percent",
      this._percent(storage.headroom_percent),
    );
    this._setText("storage-reasons", this._reasons(storage.reason_codes));

    this._setText("transition-status", this._status(transition.status));
    this._setText("transition-phase", this._transition(transition.phase));
    this._setText("current-version", this._version(transition.current_version));
    this._setText("next-version", this._version(transition.next_version));
    this._setText(
      "transition-reasons",
      this._reasons(transition.reason_codes),
    );

    this._setText("replay-status", this._status(replay.status));
    this._setText("replay-contract", this._version(replay.contract_version));
    this._setText("replay-algorithm", this._version(replay.algorithm_version));
    this._setText(
      "projection-reasons",
      this._reasons(attributes.projection_reason_codes),
    );
  }

  _setStatus(value) {
    const element = this.shadowRoot.getElementById("overall-status");
    element.dataset.status = value;
    element.textContent = value;
  }

  _setText(id, value) {
    this.shadowRoot.getElementById(id).textContent = value;
  }

  _object(value) {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return Object.freeze({});
    }
    return value;
  }

  _status(value) {
    return ["current", "stale", "unknown"].includes(value) ? value : "unknown";
  }

  _workerState(value) {
    return ["waiting", "online", "busy", "degraded", "offline", "unknown"].includes(value)
      ? value
      : "unknown";
  }

  _transition(value) {
    return ["stable", "pending", "blocked", "applying", "unknown"].includes(value)
      ? value
      : "unknown";
  }

  _alarm(value) {
    return ["clear", "warning", "critical", "unknown"].includes(value)
      ? value
      : "unknown";
  }

  _counter(value) {
    return Number.isInteger(value) && value >= 0 && value <= 1000000000
      ? String(value)
      : "unknown";
  }

  _age(value) {
    return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 31536000
      ? `${value.toFixed(1)} s`
      : "unknown";
  }

  _percent(value) {
    return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 100
      ? `${value.toFixed(1)}%`
      : "unknown";
  }

  _bytes(value) {
    return Number.isSafeInteger(value) && value >= 0
      ? `${(value / 1048576).toFixed(1)} MiB`
      : "unknown";
  }

  _freshness(value) {
    const status = this._status(value.status);
    const age = this._age(value.age_seconds);
    return age === "unknown" ? status : `${status} · ${age}`;
  }

  _hash(value) {
    const match = typeof value === "string"
      ? /^sha256:([0-9a-f]{64})$/.exec(value)
      : null;
    return match ? `sha256:${match[1].slice(0, 12)}…` : "unknown";
  }

  _version(value) {
    if (typeof value !== "string" || value.length > 64) {
      return "unknown";
    }
    const safe = /^(?:v?[0-9]+(?:\.[0-9]+){0,3}|[A-Za-z0-9][A-Za-z0-9._+-]{0,55}-v[0-9]{1,6})$/;
    return safe.test(value) ? value : "unknown";
  }

  _reasons(value) {
    if (!Array.isArray(value)) {
      return "unknown";
    }
    const safe = value
      .slice(0, 16)
      .filter((item) => typeof item === "string" && /^[a-z][a-z0-9_.:-]{0,63}$/.test(item));
    return safe.length ? safe.join(", ") : "geen";
  }
}

if (!customElements.get("stv3-worker-technical-card")) {
  customElements.define("stv3-worker-technical-card", Stv3WorkerTechnicalCard);
}

window.customCards = window.customCards || [];
if (!window.customCards.some((card) => card.type === "stv3-worker-technical-card")) {
  window.customCards.push({
    type: "stv3-worker-technical-card",
    name: "Storm Tracker V3 Worker Technical Diagnostics",
    description: "Begrensde technische workerdiagnose zonder operationele claims",
  });
}
