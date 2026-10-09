# Storm Tracker V3 — technische overdracht werksessie 2026-07-29

## Besluit

De lokale kandidaat is aantoonbaar sterker, maar **nog niet deploybaar**.
De technische P0-kern is crash-durable gemaakt en lokaal zwaar getest. De
meteorologische herbouw is veilig gestart met losse CellDetections,
CellTrack-vectors en een verliesvrije association-candidategraph in shadow
mode. Geen van die nieuwe shadowuitkomsten stuurt al een Home
Assistant-status, automatisering of legacy WeatherSystem aan.

Tijdens deze sessie zijn geen live Home Assistant-bestanden, add-ons,
MQTT-topics of databases gewijzigd. Er is geen deployarchief gebouwd en geen
database gewist.

## Lokale bewijsstatus

| Onderdeel | Lokaal resultaat | Wat dit niet bewijst |
|---|---|---|
| Volledige regressiesuite | `809 passed, 1 skipped` | geen live HA/addon/broker/power-loss |
| Durable processing | exceptionmatrix plus echte `os._exit` op 12 receive-/commitgrenzen | geen volle schijf, host power loss of pre-release migratie |
| Workerreceipts | SQLite, stable job-ID, requestdigest, restartreplay en orphan cancellation | geen echte addon-/brokercrash |
| RawArtifactStore | 24 tests, inclusief 5 echte procescrashgrenzen, concurrentie en semantische SHA-256-controle | nog niet aan providers gekoppeld; geen retentie/quota |
| CellTrack-vector | exacte tweepositiesnelheid plus 4×4 statecovariantie | twee posities bewijzen niet dat het dezelfde fysieke cel is |
| Associationgraph | ieder celpaar blijft als candidate bestaan; geen speed reject | nog geen globale assignment, split/merge of gekalibreerde probability |
| Shadowreplay | 24 frames, 2 bekende synthetische cellen, alle 92 edges behouden | geen echt weersysteem of providerhandoff bewezen |
| HA-/workerpariteit | tracking-, geometrie-, CellTrack-, graph- en raw-storemirrors gelijk | nog geen gedeeld package/artifact |

## 1. Crash-durable operationele verwerking

### 1.1 Opslaggrens

De durable tabellen zitten in de afzonderlijke Storm
Tracker-kalibratiedatabase. Ze gebruiken de Home Assistant Recorder-database
niet. Schema v3 is additief ten opzichte van de bestaande
kalibratiegegevens.

Een onbekende durable schemaversie faalt gesloten. De code voert geen
automatische destructieve migratie of stille `INSERT OR REPLACE` van
schemaversies uit.

### 1.2 Ingeststate

```text
RECEIVED
  genormaliseerde observation staat duurzaam vast
    ├─> DEDUPED
    │     runtime-deduplicatie is duurzaam en terminaal
    └─> BATCHED ─> COMMITTED
          observation zit in een immutable batch ─> batch is toegepast
```

`DEDUPED` is essentieel: zonder die staat zouden observaties die tijdens de
runtime terecht als duplicaat zijn overgeslagen na een restart opnieuw
verschijnen.

### 1.3 Batchstate

```text
PREPARED
  observation-ID's, inputdigest, regio en startrevision staan vast
    └─> EXECUTING
          attempt, worker/job-ID en startstate staan vast
            └─> RESULT_STAGED
                  resultaat valide, nog niet zichtbaar
                    └─> COMMITTED
                          state plus receipt in één SQLite-transactie

QUARANTINED
  input, reden, lineage en retrycontext blijven duurzaam aanwezig

PARTITIONED
  parent is terminaal opgesplitst; duurzame children bevatten alle input
```

Alleen `COMMITTED` betekent dat een meteorologische batch toegepast is. Een
MQTT-bericht `completed`, een worker-return of een lokale callbackreturn is op
zichzelf geen commitbewijs.

### 1.4 Statecommit

De trackingengine rekent op een working copy. Daarna controleert het journal
met compare-and-swap de verwachte startrevision. De nieuwe versioned
trackingstate en het batchreceipt worden in dezelfde transactie geschreven.
Pas na die databasecommit wordt de in-memory state geïnstalleerd.

Gevolgen:

- een late worker kan een nieuwere state niet overschrijven;
- workerfallback en lokale fallback kunnen dezelfde batch niet dubbel
  toepassen;
- een fout vóór commit laat de gepubliceerde Storm-objecten niet half
  gemuteerd achter;
- een fout na commit kan veilig hetzelfde receipt herlezen;
- twee committers op dezelfde revision hebben precies één winnaar.

### 1.5 Permanente regio-identiteit

Opslag gebruikt niet langer afgeronde latitude/longitude als permanente
identiteit. Een durable registry koppelt target-ID's en kleine GPS-jitter aan
dezelfde regio zolang afstand en observatieradius binnen het expliciete
sharingcontract blijven. Een werkelijk verre verplaatsing maakt wel een
nieuwe regio.

Bij startup staat de routebarrier per regio dicht totdat:

1. de permanente regio-ID is opgelost;
2. staged results zijn afgehandeld;
3. de laatste committed trackinghead is geïnstalleerd;
4. prepared batches en received observations zijn hersteld;
5. de backlog is geflusht;
6. `recovery_ready=True` is gezet.

Daarna volgt de globale journalintegriteitscontrole en pas na een geslaagde
controle worden providers gestart. Er bestaat in de huidige code echter een
kort technisch interval waarin de per-regio routefunctie al input kan
accepteren terwijl die globale controle nog loopt. De normale startup heeft
dan nog geen actieve providers, maar een globale admissionbarrier of het later
zetten van `recovery_ready` blijft een P0-releasegate.

## 2. Worker exact-once transportondersteuning

De worker heeft een afzonderlijke SQLite-receiptstore onder de toekomstige
addondata (`/data`):

- acceptatie wordt vóór verwerking bewaard;
- job-ID is gebonden aan jobtype en canonical payloaddigest;
- dezelfde job-ID met een andere payload faalt als
  `idempotency_conflict`;
- terminale status is immutable;
- terminale resultaten kunnen na workerrestart opnieuw worden geleverd;
- achtergelaten queued/running werk wordt bij restart expliciet
  `cancelled` met reden `worker_restarted`;
- HA gebruikt voor tracking een stabiele job-ID;
- bij transportstilte volgt hoogstens één byte-identieke QoS1-herpublicatie;
- `cancelled` beëindigt de HA-future terminaal;
- workerstatus toont of durable receipts actief zijn, de receiptdatabaseversie
  en het aantal herstelde terminale jobs.

De publieke Blitz-feed blijft een harde externe grens: zodra een
bliksemobservation de callback bereikt, wacht de callback op durable ontvangst.
De upstream feed zelf gebruikt QoS0 en kan een bericht dat vóór die callback
verdwijnt niet replayen.

## 3. CellTrack-first meteorologische herbouw

### 3.1 `CellDetection`

De nieuwe immutable detectieprimitive vereist:

- unieke detection-ID;
- geldigheidstijd;
- expliciete metrische `analysis_grid_id`;
- `x_m` en `y_m` in SI-eenheden;
- een door detector/grid aangeleverde `position_sigma_m`;
- bronidentiteit.

De primitive verzint geen universele projectie, rasterresolutie of
meetonzekerheid.

### 3.2 Twee detecties

Wanneer een latere associationlaag beslist dat twee detecties bij dezelfde
cel horen, berekent `estimate_preliminary_cell_track()` exact:

```text
vx = (x2 - x1) / Δt
vy = (y2 - y1) / Δt
speed = hypot(vx, vy)
```

Er is geen 180-km/u-cap, geen safety brake, geen smoothing en geen
WeatherSystem-centroid in deze stap. Ook een extreem resultaat blijft dus
meetbaar en auditbaar.

Dat resultaat is nog geen identiteitsbewijs. Een extreme snelheid kan echte
motion zijn, maar ook een verkeerde koppeling, providerhandoff,
projectiefout, split/merge of een andere cel.

De snapshot bevat een volledige 4×4 statecovariantie voor
`[x, y, vx, vy]`. Een korter tijdsinterval of grotere positieonzekerheid geeft
correct een grotere velocity uncertainty.

### 3.3 Lossless candidategraph

Voor twee niet-lege frames bouwt `association_graph.py` de volledige
bipartiete graph:

- iedere vorige detectie × iedere huidige detectie wordt één candidate;
- geen displacement of snelheid verwijdert een candidate;
- iedere edge bewaart exacte displacement, intervalsnelheid, bronnen,
  gecombineerde positieonzekerheid en rang;
- de caller moet motion uncertainty expliciet leveren;
- Gaussian NLL is alleen een geometrisch rangschikkingskenmerk;
- NLL wordt niet als probability of confidence gepubliceerd;
- providerhandoff blijft candidate wanneer beide producten correct op
  hetzelfde analysis grid geregistreerd zijn;
- cross-grid motion wordt niet verzonnen.

Nog niet aanwezig:

- optical-flowpredicted polygons;
- area/shape/intensity-overlapfeatures;
- globale one-to-one assignment;
- expliciete birth/deathkosten;
- split-/mergehypotheses;
- track lifecycle en gaps;
- duurzame CellTrack-/lineagetabellen;
- WeatherSystem als secundaire groepering.

### 3.4 Replay

De nieuwe contractreplay gebruikt 24 frames en twee bekende synthetische
cellen. Over 23 overgangen ontstaan `23 × 2 × 2 = 92` candidates. Alle edges
blijven behouden, ook kruis-candidates boven 1000 km/u. De twee vooraf bekende
waarheidspaden leveren exact 12 en 6 km/u.

Dit bewijst contract en rekenwerk, niet meteorologische skill. De replay
rapporteert daarom zelf:

- `fixture_kind=synthetic_known_identity`;
- `global_assignment_validated=false`;
- `real_meteorological_case=false`;
- `operational_state_mutated=false`.

## 4. Exacte upstream data

De bestaande tabel `original_radar_frames` bewaart de door een provider
gebouwde genormaliseerde overlay. Dat is nuttig en gebeurt zonder extra
regridding, maar het zijn niet voor iedere provider de oorspronkelijke HTTP-,
PNG-, HDF5-, NetCDF- of API-responsebytes.

De nieuwe geïsoleerde `RawArtifactStore` maakt de correcte toekomstige grens
mogelijk. Hij verwacht bewust een **eigen SQLite-database**; een calibration-
of Recorder-database met andere tabellen wordt als onbekend schema geweigerd.

- payload moet exact `bytes` zijn;
- SHA-256 is de contentidentity;
- identieke inhoud wordt als blob gededupliceerd;
- provider-ID plus niet-geheime upstream record-ID vormt de stabiele
  receiptidentity;
- blob en receipt committen atomair;
- dezelfde record-ID met dezelfde inhoud is idempotent;
- dezelfde record-ID met andere bytes, nominale tijd, media type of
  parser-versie faalt gesloten;
- replay controleert opnieuw lengte en SHA-256;
- `integrity_check` detecteert ook semantisch gewijzigde blobbytes wanneer
  SQLite structureel nog `ok` zegt;
- onbekende schema's worden niet overschreven;
- vijf echte `os._exit`-grenzen bewijzen niets-of-volledig commitgedrag.

De store is bewust nog niet aangesloten. Eerst vereist:

1. per provider vastleggen welk upstream artifact gezaghebbend is;
2. secretsafe record-ID-contracten;
3. worker- en HA-provideradapters;
4. quota en waarschuwingen vóór disk-full;
5. retention/archivering zonder replayverlies;
6. restore- en corruptieproeven;
7. dual-writevergelijking met de huidige overlayopslag.

## 5. Nieuwe operatorzichtbaarheid

`sensor.stv3_region_engines` kan lokaal voortaan tonen:

- globale durable processing status/integriteit;
- permanente `durable_region_id`;
- `recovery_ready` per actieve engine;
- bestaande OFE delivery-/quarantainediagnostiek.

`sensor.stv3_worker_status` neemt de volledige workerstatus over en kan dus
ook tonen:

- `durable_job_receipts`;
- `job_receipt_schema_version`;
- `recovered_terminal_jobs`;
- queue-, accepted-, terminal-, failed-, expired- en cancelledbalans;
- proceshealth/heartbeat/fout per handler.

## 6. Resterende releasegates in uitvoeringsvolgorde

### P0 — vóór enige deploy

1. Expliciet migratie-/rollbackpad voor iedere eventueel gebruikte
   pre-release durable en worker-receiptschemaversie.
2. Fault injection voor volle schijf, read-only filesystem, I/O-fout en
   SQLite-corruptie.
3. Echte HA-restart, workerrestart, addonkill, brokerdisconnect,
   QoS1-redelivery, laat resultaat en host/power-loss.
4. Langdurige live soak met queue-, disk-, memory-, CPU- en
   NUC-temperatuurtelemetrie.
5. New York-groundcase en meerdere onafhankelijke regio's replayen vanaf
   immutable echte input.
6. HA-eventside-effects en dashboarddiagnostiek live bewijzen.
7. De globale integriteitscontrole vóór iedere mogelijke routecaller laten
   slagen, niet alleen vóór providerstart.
8. Pas daarna versie-, deploy- en rollbackartefacten bouwen.

### P1 feitenlaag

1. RawArtifactStore veilig aan iedere provider koppelen.
2. Providercertificaat per product: variabele, unit, native resolutie,
   cadence, latency, dekking, nodata, QC en upstream opslagniveau.
3. `Polygon`/`MultiPolygon` met holes end-to-end bewaren.
4. `PhysicalSite`-registry voor METAR/Synoptic/MADIS/Netatmo-aliases.
5. Groundwaarden met interval, meettijd, capability, QC en independence group.

### P1 meteorologische kern

1. Metrische analysis-gridservice met checksum en halve-native-pixel
   roundtripgate.
2. Hiërarchische detector: rain shield en embedded cores.
3. Optical flow met residual/covariance.
4. Candidategraph uitbreiden met predicted footprint, overlap, shape,
   area/intensity en providerhandofffeatures.
5. Globale assignment met expliciete birth/death en alternatieven.
6. CellTrack lifecycle, gaps, split/merge en immutable lineage.
7. WeatherSystem uitsluitend als secundaire puppy-/hondenlijngroepering.
8. Probabilistische nowcast en grondfusion op dezelfde track/tijd.

### Wetenschappelijke validatie

1. Echte cases met minstens twintig bruikbare frames, niet alleen lange
   dataverzamelingen zonder relevante cel.
2. België/KMI/OPERA, Nederland/KNMI/OPERA, New York/NOAA/RainViewer en echte
   providerhandoffs.
3. Identity switches, fragmentation, track completeness, split/merge,
   displacement error, ETA CRPS/MAE, Brier Skill Score en reliability.
4. Event-level train/calibration/testscheiding zonder overlappende storms.
5. Shadowengine moet vooraf vastgelegde baselines aantoonbaar verslaan vóór
   canary of productie.

## 7. Brondocumenten

- `docs/STORM_TRACKER_ROADMAP_METEOROLOGISCH_BETROUWBAAR.md` — volledige
  architectuur, tickets, metrics, bronnen en Definition of Done.
- `docs/P0_HARDENING_CANDIDATE_1.12.0_WORKER_1.15.0.md` — lokale
  releasekandidaat, validatie en blockers.
- `docs/STORM_TRACKER_AS_BUILT_1.11.0.md` — huidige/legacy werking.
- `docs/DATA_GAP_AUDIT.md` — oorspronkelijke datagaten.

## 8. Niet verkeerd voorstellen

De correcte statuszin is:

> De lokale kandidaat heeft een crash-durable P0-verwerkingskern, een
> geïsoleerde exacte-byte opslagprimitive en een eerste lossless
> CellTrack-shadowfundering. Hij is niet live gevalideerd, niet deploybaar en
> nog geen meteorologisch gekalibreerde eindengine.
