# Canonical evidence naar lossless cellen — shadowcontract

Status: **volledig ongewired, niet operationeel en niet toegelaten**.

Dit contract sluit lokaal de eerste volledige meteorologische bewijsroute:

```text
canonical run_id
  -> CanonicalEvidenceStore.load + schema-3 raw herverificatie
  -> expliciet RadarFieldSemanticsCertificate
  -> geverifieerd providerartefact
  -> NativeRainRateFieldCandidate (mm/u, kandidaat)
  -> expliciet AffineGridTransformCertificate + receipt + config
  -> conservatieve AnalysisGridRainRateFieldCandidate
  -> NormalizedRasterFrame met alleen geldige rates
  -> lossless hierarchical detector
  -> EvidenceToCellsShadowCandidate
```

De implementatie staat uitsluitend in
`storm_tracker_worker/evidence_to_cells_shadow.py`. Er is geen import vanuit de
worker-runner, service, scheduler, providerregistry, configuratie of Home
Assistant-integratie.

## Gesloten ingang

De compositor accepteert geen losse NumPy-array, URL, bestandspad of reeds
gedecodeerd callerobject als meteorologische ingang. Verplicht zijn:

- één geldige canonical `run_id`;
- de `CanonicalEvidenceStore` die die run en zijn schema-3 raw capture opnieuw
  verifieert;
- een geverifieerde radarartefact-authority;
- een niet-lege, expliciete lijst semantics-certificaten;
- een expliciet metrisch `AnalysisGrid`;
- een expliciete gecertificeerde transformadapter en transformconfig;
- een expliciete detectorconfig;
- een expliciete pipelinecapaciteitsconfig;
- één absolute monotone deadline voor de hele keten.

De radarbridge krijgt `materialize_metric_field=False`: er ontstaat geen tweede
legacy metric-rasterroute naast de gecertificeerde transform.

De compositor voert bewust geen tweede `CanonicalEvidenceStore.load()` uit na
de bridge. De bridge heeft run en schema-3 capture dan al volledig
hergeverifieerd; een tweede load zou voor grote native captures dezelfde raw
bytes en SHA's nogmaals lezen zonder extra meteorologisch bewijs. Het resultaat
bewaart de run-, commit-, canonical receipt-, decoderresultaat-, capture
receipt- en manifestidentiteiten uit die geverifieerde bridgecandidate.

## Geen meteorologische uitspraak

Alle resultaten dragen exact:

- `admission_status = shadow_candidate_unadmitted`;
- `meteorological_admission_granted = false`;
- `wet_dry_classification_applied = false`;
- `forecast_generated = false`;
- `hard_realtime_deadline_certified = false`;
- `operational_admission_blockers` bevat verplicht
  `hard_process_isolation_not_wired`.

De shield- en corethresholds definiëren uitsluitend detectorobjecten. Ze zeggen
niet dat een locatie droog of nat is en verlenen geen product-, provider- of
operationele toelating. Ook kleine componenten verdwijnen niet: de bestaande
lossless detector bewaart ze met `low_confidence_fragment=true` wanneer hun
oppervlak onder de expliciete classificatiegrens ligt.

Voor deze fysieke rain-rateketen moet de shieldthreshold strikt groter dan
`0.0 mm/u` zijn. Een geldige kwantitatieve nul is expliciet geen
neerslagobject; een nul- of negatieve objectthreshold wordt daarom vóór enige
provider-I/O geweigerd. De positieve waarde zelf blijft caller-supplied en
contentgebonden, niet universeel of stil ingebouwd.

## Elke analysis-gridpixel blijft zichtbaar

Voor iedere pixel bestaat een immutable `AnalysisPixelAudit` op exact dezelfde
rij/kolompositie. De disposition is precies één van:

- `valid_detector_input_below_shield_threshold`;
- `valid_detector_input_rain_shield`;
- `excluded_nodata`;
- `excluded_undetect`;
- `excluded_mixed`;
- `excluded_outside_support`.

Alleen de twee `valid_detector_input_*`-toestanden krijgen een mm/u-waarde in
`NormalizedRasterFrame`. De vier uitgesloten toestanden worden `None` voor de
detector, maar behouden in het eindresultaat:

- hun expliciete disposition;
- `valid_area_fraction`;
- `nodata_area_fraction`;
- `undetect_area_fraction`;
- `outside_support_area_fraction`;
- een gegarandeerde fractiesom van 1;
- een expliciete telling in `EvidenceToCellsPixelAccounting`.

Een echte geldige `0.0 mm/u` blijft dus onderscheiden van undetect, nodata,
mixed en buiten-dekking. Voor geldige thresholdpixels wordt gecontroleerd dat
de union van detectorfootprints exact overeenkomt met alle geldige pixels boven
de shield- respectievelijk corethreshold. Geen component of pixel mag tussen de
transform en detector verdwijnen.

## Bewijs en identiteiten

Het resultaat bewaart contentgebonden identiteiten voor:

- canonical run, commit, receipt en decoderresultaat;
- raw capture, capture receipt en manifest;
- afgeleid native kandidaatveld en alle native arrays/maskers/QC/provenance;
- het volledige expliciete radar-semanticscertificaat;
- analysis-gridveld, alle maskers, fracties, QC en transformaccounting;
- het volledige transformcertificaat;
- het volledige coördinatentransformreceipt;
- de volledige transformconfig;
- het volledige detectorconfigcontract inclusief detectorversie;
- de pipelineconfig en resourcegrenzen;
- het genormaliseerde frame, alle detectiekenmerken en exacte footprints;
- ieder individueel pixelauditrecord.

Mappings worden als canonical JSON (`sort_keys`, compacte separators,
`allow_nan=false`) bewaard. Certificaat-, receipt- en config-SHA's worden opnieuw
berekend. De eind-ID is `evidence-cells-<sha256>` over de volledige output. Met
`evidence_to_cells_content_sha256()` kan een consumer dezelfde identiteit later
opnieuw berekenen; een wijziging aan een ingebed feit verandert de checksum.

## Fail-closed en resourcegrenzen

Eén absolute `deadline_at` wordt ongewijzigd doorgegeven aan radarbridge en
analysis-gridtransform. De compositor controleert dezelfde deadline voor en na
iedere fase en periodiek tijdens pixelauditing. De caller mag de deadline niet
verder leggen dan `max_runtime_seconds` en nooit verder dan de harde 120 s.

De gesloten harde bovengrenzen zijn:

- 250.000 targetpixels;
- 500.000 detectorobjecten;
- 64 MiB canonical eindreceipt;
- 120 s aangevraagde runtime.

Lagere callergrenzen zijn verplicht via `EvidenceToCellsShadowConfig`. De
transform behoudt daarnaast zijn eigen sourcepixel-, targetpixel-, arraybyte-
en overlapbijdragegrenzen. Deadline- of capaciteitsovertredingen leveren een
specifieke exception; bewijs-, bindings- of transformproblemen leveren een
resultaat zonder kandidaat en met een expliciete stage/code/detail. Er wordt
nooit een gedeeltelijke kandidaat gepubliceerd.

Vóór provider-I/O wordt `target_pixels` zowel tegen de pipeline- als
transformlimiet gecontroleerd. Omdat de lossless hiërarchie maximaal één
shield en één core per targetpixel kan produceren, wordt ook de conservatieve
bovengrens `2 * target_pixels` vooraf tegen het objectbudget gecontroleerd. De
eindreceipt wordt incrementeel als canonical JSON gehasht; de bytegrens en
deadline worden tijdens die stream gecontroleerd zonder een tweede volledig
JSON-bytebuffer te alloceren.

`detect_hierarchical_cells()` ontvangt nu dezelfde absolute monotone deadline
en hetzelfde expliciete objectbudget. De raster-, component-, footprint- en
kenmerkenlussen controleren die deadline coöperatief; een overschrijding levert
geen gedeeltelijk frame. Het objectbudget wordt bij ontdekking van iedere
shield of core afgedwongen en kan dus geen staart stil verwijderen. Dit blijft
bewust geen hard-real-time certificaat: procesisolatie met een parent-owned
hard kill, CPU/RSS-limieten en een complete effectgrens is nog niet met deze
compositor gewired. Daarom blijft `hard_process_isolation_not_wired` de
expliciete operationele blocker.

## Geteste eigenschappen

De synthetische tests bewijzen lokaal:

1. committed schema-3 bewijs -> native rate -> gridtransform -> lossless shield
   en core;
2. behoud van echte nul, nodata en exacte detectiefootprints;
3. gelijktijdig expliciet behoud van valid, mixed, nodata, undetect en
   outside-support met oppervlaktefracties;
4. fail-closed bij artefact/capture-bindingtamper;
5. herberekenbare outputidentiteit bij post-creation tamper;
6. deadline- en capaciteitstop vóór providerwerk;
7. afwezigheid van loose-array-ingang en live wiring.

## Open gates vóór enige wiring

Deze compositor verandert geen productstatus. Voor een latere shadow-run zijn
nog minimaal nodig:

- uitsluitend providerproducten met een toegelaten decoder en exact officieel
  semantics-certificaat;
- officiële grid/projection/QC-certificaten in plaats van synthetische
  certificaten;
- representatieve multi-regio- en multi-providerreplays met ground evidence;
- fault-, crash-, storage-, thermische en deadlineproeven op echte
  providerafmetingen;
- een HA/Z2M-soak die bewijst dat alle zware I/O en CPU buiten Home Assistant
  blijven;
- expliciete shadow/canarybeslissing en afzonderlijke meteorologische
  toelatingslogica. Die toelatingslogica hoort niet in deze module.
