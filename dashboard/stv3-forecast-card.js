/* Storm Tracker forecast result card v1.1.0 — dashboard-only, NAS-HA */
class StormTrackerForecastCard extends HTMLElement {
  setConfig(config) {
    this._config = {
      entity: "sensor.storm_tracker_fictieve_tracker_neerslag",
      target_id: "test_tracker",
      title: "Actuele voorspelling",
      ...config,
    };
    if (!this.shadowRoot) this.attachShadow({ mode: "open" });
    this._render();
  }
  set hass(hass) {
    this._hass = hass;
    this._ensureSubscription();
    this._render();
  }
  connectedCallback() { this._ensureSubscription(); }
  disconnectedCallback() {
    const pending = this._unsubscribePromise;
    this._unsubscribePromise = null;
    if (pending) pending.then((unsubscribe) => {
      if (typeof unsubscribe === "function") unsubscribe();
    }).catch(() => {});
  }
  getCardSize() { return 3; }
  _ensureSubscription() {
    if (!this.isConnected || !this._hass || this._unsubscribePromise) return;
    this._unsubscribePromise = this._hass.connection.subscribeMessage(
      (message) => {
        const event = message && message.event ? message.event : message;
        try {
          const value = JSON.parse(event.payload);
          if (value && [1, 2].includes(value.schema_version) && Array.isArray(value.items)) {
            this._passages = value;
            this._error = null;
          } else {
            this._error = "ongeldig resultaatcontract";
          }
        } catch (_err) {
          this._error = "resultaat niet leesbaar";
        }
        this._render();
      },
      { type: "mqtt/subscribe", topic: "storm_tracker/v1/operational/passages" },
    ).catch((err) => {
      this._error = err && err.message ? err.message : "resultaatstroom niet bereikbaar";
      this._unsubscribePromise = null;
      this._render();
      return null;
    });
  }
  _escape(value) {
    return String(value ?? "").replace(/[&<>"']/g, (char) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    })[char]);
  }
  _time(epochSeconds) {
    if (epochSeconds === null || epochSeconds === undefined || epochSeconds === "") return "onbekend";
    if (!Number.isFinite(Number(epochSeconds))) return "niet beschikbaar";
    return new Date(Number(epochSeconds) * 1000).toLocaleTimeString("nl-BE", {
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    });
  }
  _passageDuration(item) {
    if (item?.entry_at === null || item?.entry_at === undefined
      || item?.exit_at === null || item?.exit_at === undefined) return "onbekend";
    const seconds = Number(item?.exit_at) - Number(item?.entry_at);
    if (!Number.isFinite(seconds) || seconds < 0) return "niet beschikbaar";
    return seconds < 60 ? `${Math.round(seconds)} sec` : `${(seconds / 60).toFixed(1)} min`;
  }
  _statusLabel(status) {
    return ({candidate:"voorlopig",confirmed:"bevestigd",coasting:"coasting / onzeker",
      current:"actueel",ended:"afgelopen",cancelled:"geannuleerd",
      indeterminate:"onbepaald"})[status] || status || "onbekend";
  }
  _bracket(bracket) {
    if (!bracket || bracket.lower_at == null || bracket.upper_at == null) return "onbekend";
    return `${this._time(bracket.lower_at)}–${this._time(bracket.upper_at)}`;
  }
  _numberOrNull(value) {
    if (value === null || value === undefined || value === "") return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }
  _upcomingPassages(items) {
    if (!items.length) return "";
    const visible = items.slice(0, 3);
    const rows = visible.map((item, index) => `
      <div class="upcoming-item" data-cell-id="${this._escape(item.cell_id || "onbekend")}">
        <b>${index + 1}.</b>
        <span>${this._escape(this._time(item.entry_at))}–${this._escape(this._time(item.exit_at))}</span>
        <span>${this._numberOrNull(item.predicted_mean_intensity_mm_h ?? item.predicted_intensity_mm_h) !== null ? `${this._numberOrNull(item.predicted_mean_intensity_mm_h ?? item.predicted_intensity_mm_h).toFixed(1)} mm/u` : "intensiteit onbekend"}</span>
      </div>`).join("");
    const overflow = items.length > visible.length
      ? `<div class="upcoming-more">+${items.length - visible.length} latere passage(s)</div>`
      : "";
    return `<div class="upcoming"><b>Aankomende passages</b>${rows}${overflow}</div>`;
  }
  _render() {
    if (!this.shadowRoot || !this._config) return;
    const state = this._hass && this._hass.states[this._config.entity];
    const attrs = state ? state.attributes || {} : {};
    const items = this._passages && Array.isArray(this._passages.items) ? this._passages.items : [];
    const evaluated = Number(this._passages?.evaluated_at);
    const targetItems = items.filter((entry) => entry && entry.target_id === this._config.target_id)
      .sort((left, right) => Number(left.entry_at) - Number(right.entry_at));
    const isRaining = attrs.current_precipitation_state === "raining" || (state && state.state === "regen");
    const ongoing = Number.isFinite(evaluated)
      ? targetItems.find((entry) => entry.status === "current" || (
        entry.entry_at != null && entry.exit_at != null
        && Number(entry.entry_at) <= evaluated && Number(entry.exit_at) >= evaluated
      ))
      : undefined;
    const item = ongoing || (!isRaining ? targetItems[0] : undefined);
    const upcoming = targetItems.filter((entry) => (
      Number.isFinite(evaluated)
      && Number(entry.entry_at) > evaluated
      && entry !== item
    ));
    const reasons = Array.isArray(attrs.next_threat_reason_codes) ? attrs.next_threat_reason_codes : [];
    let body;
    if (isRaining) {
      const intensity = Number(attrs.rain_rate_mm_h);
      const predicted = this._numberOrNull(ongoing?.predicted_mean_intensity_mm_h ?? ongoing?.predicted_intensity_mm_h);
      const displayedIntensity = Number.isFinite(intensity)
        ? intensity
        : predicted;
      const evaluatedAt = this._passages && this._passages.evaluated_at;
      const age = Number.isFinite(Number(evaluatedAt)) ? Math.max(0, Date.now() / 1000 - Number(evaluatedAt)) : null;
      const freshness = age === null ? "tijd onbekend" : (age <= 300 ? `actueel · ${Math.round(age)} sec oud` : `⚠️ ${Math.round(age / 60)} min oud`);
      body = `
        <div class="status">${this._escape(attrs.status_text || "Regen gedetecteerd")}</div>
        <div class="grid">
          <div><span>Aankomst</span><strong>bezig</strong></div>
          <div><span>Einde</span><strong>${ongoing ? this._escape(this._time(ongoing.exit_at)) : "nog niet berekend"}</strong></div>
          <div><span>Duur</span><strong>${ongoing ? this._escape(this._passageDuration(ongoing)) : "nog niet berekend"}</strong></div>
          <div><span>Intensiteit</span><strong>${displayedIntensity === null ? "onbekend" : `${displayedIntensity.toFixed(1)} mm/u`}</strong></div>
        </div>
        <div class="detail"><b>Reden/basis:</b> ${this._escape(reasons.length ? reasons.join(", ") : "actuele neerslagmeting")}</div>
        ${this._upcomingPassages(upcoming)}
        <div class="fresh">${this._escape(freshness)}</div>`;
    } else if (item) {
      const duration = this._passageDuration(item);
      const statusLabel = this._statusLabel(item.status);
      const entryBracket = item.entry_bracket ? this._bracket(item.entry_bracket) : null;
      const exitBracket = item.exit_bracket ? this._bracket(item.exit_bracket) : null;
      const uncertainty = item.fallback_reason
        ? `fallback: ${item.fallback_reason}`
        : (item.horizon_clipped ? "voorspelling eindigt op de modelhorizon" : "geen fallback; horizon niet afgeknipt");
      const age = Number.isFinite(evaluated) ? Math.max(0, Date.now() / 1000 - evaluated) : null;
      const freshness = age === null ? "tijd onbekend" : (age <= 300 ? `actueel · ${Math.round(age)} sec oud` : `⚠️ ${Math.round(age / 60)} min oud`);
      body = `
        <div class="status">${this._escape(attrs.status_text || (state && state.state) || "voorspelling beschikbaar")} · ${this._escape(statusLabel)}</div>
        <div class="grid">
          <div><span>Aankomst</span><strong>${this._escape(this._time(item.entry_at))}</strong></div>
          <div><span>Einde</span><strong>${this._escape(this._time(item.exit_at))}</strong></div>
          <div><span>Duur</span><strong>${this._escape(duration)}</strong></div>
          <div><span>Intensiteit</span><strong>${this._numberOrNull(item.predicted_mean_intensity_mm_h ?? item.predicted_intensity_mm_h) !== null ? `${this._numberOrNull(item.predicted_mean_intensity_mm_h ?? item.predicted_intensity_mm_h).toFixed(1)} ${this._escape(item.unit || "mm/u")}` : "onbekend"}</strong></div>
        </div>
        ${entryBracket ? `<div class="detail"><b>Aankomstvenster:</b> ${this._escape(entryBracket)}</div>` : ""}
        ${exitBracket ? `<div class="detail"><b>Vertrekvenster:</b> ${this._escape(exitBracket)}</div>` : ""}
        ${item.right_censored ? `<div class="detail"><b>Vertrek:</b> buiten de rekenhorizon; eindtijd onbekend</div>` : ""}
        <div class="detail"><b>Onzekerheid:</b> ${this._escape(uncertainty)}</div>
        <div class="detail"><b>Reden/basis:</b> ${this._escape(reasons.length ? reasons.join(", ") : (item.method || "niet opgegeven"))} · methode ${this._escape(item.method || "onbekend")}</div>
        ${this._upcomingPassages(upcoming)}
        <div class="fresh">${this._escape(freshness)}</div>`;
    } else {
      const rawEta = attrs.next_threat_eta_minutes;
      const rawReference = attrs.next_threat_product_valid_at ?? attrs.product_valid_at;
      const eta = rawEta === null || rawEta === undefined || rawEta === "" ? NaN : Number(rawEta);
      const reference = rawReference === null || rawReference === undefined || rawReference === "" ? NaN : Number(rawReference);
      const arrival = Number.isFinite(eta) && Number.isFinite(reference) ? this._time(reference + eta * 60) : "nog niet berekend";
      const presentationReasons = Array.isArray(attrs.presentation_reason_codes) ? attrs.presentation_reason_codes : [];
      const reason = reasons.length ? reasons.join(", ") : presentationReasons.join(", ") || this._error || "wacht op een gevalideerde passage";
      body = `
        <div class="status">${this._escape(attrs.status_text || (state && state.state) || "wacht op voorspelling")}</div>
        <div class="grid">
          <div><span>Aankomst</span><strong>${this._escape(arrival)}</strong></div>
          <div><span>Einde / duur</span><strong>nog niet berekend</strong></div>
        </div>
        <div class="detail"><b>Onzekerheid/reden:</b> ${this._escape(reason)}</div>`;
    }
    this.shadowRoot.innerHTML = `
      <style>
        :host{display:block} ha-card{padding:14px 16px}
        h2{font-size:17px;line-height:1.2;margin:0 0 6px}.status{color:var(--secondary-text-color);margin-bottom:12px}
        .grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-bottom:10px}
        .grid div{background:var(--secondary-background-color);border-radius:10px;padding:9px 10px;min-width:0}
        span{display:block;color:var(--secondary-text-color);font-size:12px;margin-bottom:2px}strong{display:block;font-size:15px;overflow-wrap:anywhere}
        .detail{font-size:13px;line-height:1.45;margin-top:5px;overflow-wrap:anywhere}.fresh{font-size:11px;color:var(--secondary-text-color);margin-top:8px}
        .upcoming{margin-top:12px;padding-top:9px;border-top:1px solid var(--divider-color);font-size:13px}.upcoming>b{display:block;margin-bottom:5px}.upcoming-item{display:grid;grid-template-columns:auto 1fr auto;gap:7px;align-items:center;padding:4px 0}.upcoming-item span{font-size:12px;margin:0}.upcoming-more{font-size:12px;color:var(--secondary-text-color);margin-top:3px}
      </style>
      <ha-card><h2>⛈️ ${this._escape(this._config.title)}</h2>${body}</ha-card>`;
  }
}
if (!customElements.get("storm-tracker-forecast-card")) {
  customElements.define("storm-tracker-forecast-card", StormTrackerForecastCard);
}
window.customCards = window.customCards || [];
if (!window.customCards.some((card) => card.type === "storm-tracker-forecast-card")) {
  window.customCards.push({ type: "storm-tracker-forecast-card", name: "Storm Tracker voorspelling", description: "Actuele aankomst, eindtijd, duur en onzekerheid" });
}
