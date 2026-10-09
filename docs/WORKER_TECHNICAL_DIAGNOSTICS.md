# Worker technical diagnostics

## Huidige status

`worker_technical_diagnostics.py` is een pure, begrensde projector. De module
leest geen Home Assistant-state, Worker App-configuratie, MQTT, bestand, database
of netwerkbron. De worker-only runtime is nu als volgt bedraad:

- de Worker App publiceert in zijn bestaande status precies één gesloten
  `technical_diagnostics`-bron volgens source-v1;
- `worker_bridge.py` valideert die bron exact, bindt staat en ontvangsttijd aan
  de lokale bridge-observatie en bewaart alleen de genormaliseerde bron en
  projectie;
- een begrensd intern event bevat alleen projectiestatus en projectietijd;
- elke worker-only entry composeert precies één event-driven, niet-pollende
  technische diagnose-entity;
- unload verwijdert de listener en ruimt bron en projectie op nadat de laatste
  runtime-eigenaar verdwenen is.

`dashboard/stv3-worker-technical-card.js` blijft een standalone kaartbestand.
Het is bewust nog niet als Lovelace-resource geregistreerd en dus nog niet
automatisch aan een dashboard toegevoegd. Ontbrekende autoritatieve inputs
blijven `unknown`; de wiring maakt geen availabilityclaim namens de worker.

## Broncontract

De projector accepteert uitsluitend een mapping met
`contract: stv3-worker-technical-diagnostics-source-v1`. Een ontbrekende mapping
geeft `diagnostic_source_missing`; een ontbrekend of afwijkend contract geeft
`diagnostic_source_contract_invalid`. Beide leveren het vaste unknown-document.

De pure projector leest uitsluitend de onderstaande sleutels en kopieert nooit
een onbekend veld. De bridge-normalizer is strenger: op de wire moet source-v1
exact dit gesloten schema hebben; elke extra of ontbrekende sleutel maakt de
volledige bron ongeldig.

| Sleutel | Geaccepteerde waarde | Betekenis |
| --- | --- | --- |
| `worker_state` | `waiting`, `online`, `busy`, `degraded`, `offline`, `unknown` | Technische workerstaat |
| `worker_observed_at` | eindige Unix-timestamp >= 0 | Tijd van de gebonden heartbeatobservatie |
| `worker_version` | numerieke versie of begrensde `...-vN`-identifier | Release-identiteit, geen vrije tekst |
| `build_sha256` | exact SHA-256-contentadres | Buildidentiteit |
| `config_sha256` | exact SHA-256-contentadres | Identiteit van een geheimvrije configuratieprojectie |
| `desired_state_sha256` | exact SHA-256-contentadres | Identiteit van de toegepaste desired state |
| `provider_age_seconds`, `frame_age_seconds`, `cache_age_seconds` | eindig getal van 0 t/m 31.536.000 | Door de worker berekende technische leeftijd |
| `provider_status`, `frame_status`, `cache_status` | `current`, `stale`, `unknown` | Expliciete freshnessbeoordeling van de worker |
| `queue_depth`, `terminal_jobs`, `failed_jobs` | geheel getal >= 0 | Begrensde jobtellers |
| `retention_alarm`, `capacity_alarm` | `clear`, `warning`, `critical`, `unknown` of bool | Technische alarmklasse; bool wordt naar `clear`/`critical` vertaald |
| `headroom_bytes` | geheel getal van 0 t/m 2^63-1 | Expliciet aangeleverde capaciteitheadroom |
| `headroom_percent` | eindig getal van 0 t/m 100 | Expliciet aangeleverd headroompercentage |
| `storage_reason_codes` | iterable van veilige codes | Retentie-/capaciteitsredenen |
| `transition_status` | `stable`, `pending`, `blocked`, `applying`, `unknown` | Current-next-fase |
| `current_version`, `next_version` | numerieke versie of begrensde `...-vN`-identifier | Current-next-identiteiten |
| `transition_reason_codes` | iterable van veilige codes | Current-next-redenen |
| `replay_contract_version`, `replay_algorithm_version` | numerieke versie of begrensde `...-vN`-identifier | Replaycontract en -algoritme |

Een hash wordt alleen doorgegeven wanneer de bron al een exact volledig
SHA-256-contentadres levert. HA hasht geen willekeurige callerdata. In het
bijzonder mag `config_sha256` nooit zijn afgeleid van credentials, tokens of
andere geheimen; de Worker App moet eerst een expliciete geheimvrije
configuratieprojectie maken.

## Uitvoercontract

De uitvoer heeft altijd
`contract: stv3-ha-worker-technical-diagnostics-v1` en exact deze vaste groepen:

```text
contract
source_contract
status                         current | stale | unknown
projected_at
projection_reason_codes
worker                         state, observed_at, age_seconds, version,
                               build/config/desired-state SHA-256
freshness.provider|frame|cache status, age_seconds
jobs                           status, queue_depth, terminal_jobs, failed_jobs,
                               counts_saturated
storage                        status, retention_alarm, capacity_alarm,
                               headroom_bytes, headroom_percent, reason_codes,
                               reason_codes_complete
transition                     status, phase, current_version, next_version,
                               reason_codes, reason_codes_complete
replay                         status, contract_version, algorithm_version
claims                         meteorological_truth=false,
                               operational_admission=false, alert=false
```

`status` zegt alleen of de technische projectie actueel genoeg is om te tonen.
Het is geen service-SLO, providerbeschikbaarheid, capture admission of
weerwaarschuwing.

### Current, stale en unknown

- `worker_observed_at` ontbreekt of is ongeldig: `unknown`.
- `worker_state` ontbreekt of is ongeldig: `unknown`.
- Een heartbeat meer dan 5 seconden in de toekomst: `unknown`.
- `waiting`, `offline` en elke andere niet-live staat: `stale`.
- `online`, `busy` of `degraded` met een heartbeatleeftijd boven 35 seconden:
  `stale`.
- Alleen een live staat met een heartbeatleeftijd van maximaal 35 seconden is
  `current`.
- Elke bekende subgroep erft globale `stale`; bij globale `unknown` is elke
  subgroep `unknown`.
- Provider-, frame- en cachefreshness vereist zowel een geldige leeftijd als een
  expliciete bronstatus. Ontbreekt een van beide, dan is die freshness `unknown`.
- Een ontbrekend of ongeldig individueel veld blijft `null`/`unknown`; een ander
  veld in dezelfde groep wordt niet gebruikt om het te raden.

### Begrenzing

- Jobtellers satureren op 1.000.000.000 en zetten `counts_saturated=true`.
- Per lijst worden hoogstens 17 items gelezen en hoogstens 16 unieke, veilige
  redencodes uitgegeven. Overflow, een duplicaat of een ongeldige code zet
  `reason_codes_complete=false`.
- Hashes en versie-identifiers hebben vaste maximale formaten.
- Canonieke JSON gebruikt gesorteerde sleutels, ASCII, geen NaN/Infinity en geen
  witruimte.
- Het harde uitvoerbudget is 65.536 bytes. Een onverwachte budgetoverschrijding
  valt terug op het vaste unknown-document.
- De hoeveelheid werk en uitvoer is O(1) ten opzichte van callerdata.

## Privacy- en autoriteitsgrens

De projectie bevat geen providernaam, providerwaarde, payload, pad, topic, URL,
credential, wachtwoord of token. Onbekende sleutels en geneste callerdata worden
genegeerd. Redencodes en versies zijn begrensde identifiers, geen fouttekst.

De velden `retention_alarm` en `capacity_alarm` zijn technische signalen. De
projector leest of publiceert geen `admission_ready`, capturebeslissing of
alertstatus. De drie velden onder `claims` staan daarom onvoorwaardelijk op
`false`. Er worden evenmin meteorologische waarden, neerslagtoestand,
waarschijnlijkheid, ETA of dreigingsbeslissingen afgeleid.

### Werkelijk aangeleverde workerbron

Source-v1 levert momenteel uitsluitend wat de Worker App zelf autoritatief kan
bewijzen:

- workerstaat, lokaal gebonden observatietijd en workerversie;
- de volledige SHA-256 van de worker-codeboom als build-contentadres;
- een desired-state-SHA-256 alleen wanneer `last_tick` exact die identiteit
  bevat;
- begrensde wachtrij-, terminale-job- en fouttellers;
- retentie- en capaciteitstatus uit de hieronder beschreven guard-v2;
- geen configuratiehash zolang geen expliciete geheimvrije
  configuratieprojectie bestaat.

Provider-, frame- en cacheleeftijd, current-vs-next-transitie en replaycontract
blijven momenteel volledig `unknown`. De workerversie wordt niet hergebruikt als
current-vs-next-bewijs en een build-id wordt nooit als SHA-256 voorgesteld.

### Guard-v2-retentie en capaciteit

De storageprojectie leest uitsluitend
`_operational_snapshot_health["retention_guard"]`. Ze accepteert alleen het
exacte contract `stv3-worker-raw-capture-retention-guard-v2`, het exacte
`config_bound_optional_event_envelope_reserved_fail_closed_v2`-beleid, het
gesloten top-, ownership-, reclamation- en capacityschema, een voltooide audit
en de vereiste eigendoms- en padreproof. Elke afwijking maakt de volledige
storagegroep `unknown`.

Bij een geldige guard gelden de volgende vaste projectieregels:

- geen alarmcode: `clear`;
- alleen bekende lage-capaciteits- of transportcodes: `warning`;
- een bekende exhausted- of integriteitscode: `critical`;
- `reason_codes` zijn exact de begrensde, geordende guardcodes;
- `headroom_bytes` is het minimum van de bewezen cadence-catalogus-,
  cadence-ledger-, cadence-ledgercatalogus-, cadence-raw- en fysieke
  byte-headrooms;
- `headroom_percent` blijft `null`, omdat guard-v2 geen gemeenschappelijke,
  bewezen noemer levert.

De bridge projecteert nooit `admission_ready`, event-capacityclaims,
bestandspaden, topics, payloads of vrije fouttekst.

## Standalone dashboardkaart

`stv3-worker-technical-card.js`:

- vereist een expliciete `entity` en raadt geen entity-id;
- accepteert alleen het exacte HA-uitvoercontract;
- leest alleen de vaste groepen hierboven;
- begrenst redencodes tot 16 en verkort een gevalideerd SHA-256-contentadres voor
  weergave;
- zet alle dynamische tekst via `textContent` in de DOM;
- doet geen API-call, service-call, abonnement, netwerk- of MQTT-actie;
- toont permanent dat de kaart geen meteorologische waarheid, operationele
  toelating of waarschuwing levert.

Omdat de Lovelace-resource nog niet geregistreerd is, hoort de kaart nog niet
in een productie-dashboardconfiguratie.

## Afzonderlijke handoff voor de resterende dashboardregistratie

De Pythonwiring hierboven is gesloten. Voor zichtbaarheid in Lovelace zijn nog
exact deze dashboardhandelingen nodig; daarvoor hoeft geen meteorologische,
credential- of workerstatuswiring te worden gewijzigd.

1. Kopieer de kaart bij de dashboarddeploy naar
   `/config/www/stv3-worker-technical-card.js`, registreer pas na een bestaande
   entity een Lovelace-module-resource op
   `/local/stv3-worker-technical-card.js?v=<release>` en configureer daarna:

   ```yaml
   type: custom:stv3-worker-technical-card
   entity: sensor.<werkelijke_worker_technical_entity>
   ```

2. Leg de resourcewijziging vast in de bestaande dashboard-/resourceconfig en
   voeg daar een statische resourcetest aan toe. Neem kaart en nieuwe
   integratiebestanden daarna in de release- en deploypinset op.

De kaart moet de expliciete entity blijven vereisen; er mag geen geraden entity,
defaultstub of verborgen API-pad worden toegevoegd.
