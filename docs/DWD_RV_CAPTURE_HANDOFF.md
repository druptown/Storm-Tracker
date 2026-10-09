# DWD RV raw-v2 naar canonical shadow-evidence

Status: lokale, niet-live shadowkandidaat. Er is geen import vanuit
`service.py`, geen MQTT/HA-publicatie, geen scheduler- of science-orchestrator-
wiring en geen operationele admission.

## Waarom deze grens nodig was

De acquisitieruntime bewaart DWD RV bewust als exact raw transportbewijs:

- provider `dwd`;
- raw product `dwd_open_data_composite_rv_tar`;
- captureproduct `dwd_open_data_composite_rv_raw_capture`;
- collectorversie `worker-dwd-rv-tar-exact-raw-v2`.

De bestaande gecertificeerde kwantitatieve decoder verwachtte de oudere
canonical capture-identiteit `dwd_radolan` /
`dwd_open_data_composite_rv_capture`. Daardoor kon de nieuwe veilige acquisitie
niet rechtstreeks naar de decoder zonder ofwel provenance te verliezen, ofwel
de decodercontrole losser te maken.

`storm_tracker_worker/dwd_rv_capture_handoff.py` sluit dit met een expliciet,
versieerbaar contract:

- schema `stv3-worker-dwd-rv-capture-handoff-v1`;
- adapter `dwd.rv.raw-capture-v2-to-observation-capture`, versie `v1`;
- target collector `worker-dwd-rv-observation-handoff-v1`;
- een contenthash van de volledige adapterconfiguratie;
- een afzonderlijke caller-owned schema-3 handoffstore.

De decoder houdt zijn bestaande legacy-v1-profiel uitsluitend voor historische
replays en accepteert daarnaast precies dit tweede gesloten profiel. Een
onbekende collectorversie, ontbrekend adapterveld of gewijzigde inventory
resulteert in `unsupported:capture_handoff`. Er is geen generieke alias,
providerfallback of soepeler provenancepad.

## Append-only en crash-durable

De adapter doet geen netwerkverzoek en kan dus nooit refetchen. Per aanroep:

1. leest en verifieert hij de bestaande raw-v2 capture, receipt en exacte tar;
2. valideert hij provider, producten, fetchbinding, upstreamidentiteiten,
   ontvangsttijd, collectorversie en producttimestamp;
3. inspecteert en hasht hij ieder tarlid begrensd in memory, zonder extractie;
4. schrijft hij dezelfde tarbytes naar een afzonderlijke content-addressed
   handoffstore;
5. commit hij een deterministische `dwd_radolan`-capture;
6. leest hij die targetcapture opnieuw volledig en vergelijkt bytes, hashes en
   inventory met de sourcecapture.

Source en target hebben dezelfde archive-SHA-256. De sourcecapture wordt nooit
gewijzigd of verwijderd. De handoffstore is bewust gescheiden van de
acquisitie-rawstore: de nog niet live aangesloten adapter kan zo geen verborgen
receipt- of ledgeraccounting aan de acquisitieruntime toevoegen.

Receipt- en capturefetch-ID's zijn deterministisch uit de source-capture-ID
afgeleid. Een crash na sourcevalidatie, targetreceipt of targetcapture wordt op
restart exact idempotent hervat. Twee gelijktijdige adapters publiceren door de
schema-3 CAS-contracten één receipt, één capture en één raw object.

## Observatie en forecast worden niet gemengd

Volgens de bestaande officiële DWD RV-product- en Open Data-documentatie is de
driecijferige suffix de voorspellingsstap. De adapter vereist daarom exact één
archivebasis met alle vijfminutenleden van `+000` tot `+120`:

- uitsluitend `..._000-hd5` wordt als observation member gebonden;
- `..._005-hd5` tot en met `..._120-hd5` blijven 24 afzonderlijke forecast
  members;
- ieder lid krijgt naam, lead, bytegrootte en eigen SHA-256;
- de forecastlijst en volledige memberlijst krijgen elk een inventory-SHA;
- forecastbeleid is letterlijk `retained_exact_raw_separate_not_decoded`.

De adapter opent geen HDF5-lid en kent geen quantity, unit of gridwaarde toe.
De kwantitatieve decoder leest daarna uitsluitend `+000`. Forecastleden worden
niet geselecteerd, gemiddeld, samengevoegd of als extra observatietijden
gepresenteerd. Een ontbrekend, dubbel, verkeerd genoemd of buiten het gesloten
`+000..+120/5`-profiel vallend lid faalt gesloten.

## Meteorologische validatie na de handoff

Na de identiteitsadapter voert de ongewijzigde DWD kwantitatieve decoder de
inhoudelijke validatie uit. De echte capture bewijst onder meer:

- `ODIM_H5/V2_3`, object `COMP` en product `MAX` / `RV_top_view`;
- quantity `ACRR`, eenheid mm en exact 300 seconden accumulatie;
- gain circa `0.001`, bijbehorende offset, `nodata` en `undetect`;
- producttijd = intervaleinde = bestandsnaamtijd van `+000`;
- raster 1200 rijen × 1100 kolommen, 1 km × 1 km;
- het gesloten WGS84-polairstereografische CRS-profiel en de vier
  rasterhoeken;
- afzonderlijke valid-, nodata- en undetectmaskers.

Alleen na een volledige decode ontstaat een immutable quantitative artifact en
`decoded_complete` canonical evidence. De bestaande canonical radar bridge zet
de exacte vijfminutenaccumulatie daarna om naar intervalgemiddelde mm/u. De
bestaande officiële DWD DE1200-gridcertificate kan dat veld zonder
CRS-herinterpretatie naar een expliciet targetgrid brengen, waarna de bestaande
evidence-to-cells-shadowcompositor het accepteert.

Dit blijft een shadowkandidaat. `forecast_generated=false`, admission blijft
geblokkeerd en er wordt niets gepubliceerd.

## Vaste echte fixture

De vaste compatibiliteitsproef is `.analysis/dwd-rv-live.tar`:

- 880.640 bytes;
- SHA-256
  `08e0ef2539dde3c40d3d35d0f8750420d5f4c902712968abdcf47ffad583d8f5`;
- basistijd `2026-08-01T23:05:00Z`;
- 25 leden `+000..+120`;
- observation-member-SHA-256
  `5d776fb59c2192ccbdb0420c189e5cd7c01eb52245aac8108ac2485f1a7ad7c3`.

`tests/fixtures/dwd_rv_live_evidence.json` bindt de exacte naam, lead,
bytegrootte en SHA-256 van ieder lid. De end-to-endtest gebruikt de tar zelf,
niet een synthetisch HDF5-profiel.

## Wat dit bewijs niet betekent

De archive heeft één basistijd en levert precies één observatieframe aan deze
keten. Dat bewijst decoder-, quantity-, tijd-, grid- en lineagecompatibiliteit,
maar **geen bewegingsvaardigheid**. Motion vereist meerdere onafhankelijke
observatietijden, correcte tijdsafstand, stabiele provider/grididentiteit en
held-out bewegingsreplays. De 24 forecastleden zijn geen vervanging voor zo'n
observatiegeschiedenis en mogen daar nooit als input voor worden gebruikt.

Resterend vóór livegebruik:

- expliciete scheduler/process-isolation/admissionwiring voor de decodeketen;
- de complete handoff is nu in de bounded
  `acquisition_evidence_lifecycle.py`-mirror archiveerbaar; veilige reclaim van
  de oorspronkelijke raw-, handoff-, scheduler-, execution- en ledgerstores
  wacht nog op archive-aware tombstones/resolvers;
- multi-frame en multi-event replaybewijs voor motion/cellen;
- thermische NUC-soak en onafhankelijke grondvalidatie;
- aparte forecastsemantiek en forecastskillvalidatie indien de `+005..+120`
  leden ooit buiten raw lineage gebruikt zouden worden.
