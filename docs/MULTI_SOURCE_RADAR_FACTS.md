# Multi-source radar target facts

## Doel en grens

`storm_tracker_worker.multi_source_radar_facts` maakt de eerste echte,
provider-neutrale meteorologische bijdrage voor expliciete DesiredState-targets.
De laag consumeert uitsluitend de reeds door
`canonical_radar_field_bridge` geverifieerde
`NativeRainRateFieldCandidate` en de bestaande, gesloten
`RadarFieldSemanticsCertificate`.

De module doet geen netwerkverkeer, bestands- of database-I/O, scheduling,
providerfallback, ruimtelijke fusie, voorspelling, waarschuwing of operationele
admission. Daardoor kan de latere multi-source cycle de output als één getypeerd,
content-addressed bewijs opnemen zonder een importcirkel of een tweede decoder te
introduceren.

## Exact toegelaten kwantitatieve bronnen

De allowlist is bewust exact vier entries groot. Een candidate moet byte- en
hashmatig overeenkomen met de officiële semantics certificate; provider,
product, decoderidentiteit, decoderversie, decoderconfig, quantity, unit,
temporal semantics, QC-state en primaire bronnen moeten allemaal overeenkomen.
Alle vier leveren daarom source-level kwantitatieve evidence. Targetsampling
blijft daarnaast afhankelijk van een afzonderlijk bewezen pixel-edge authority.

- `radar.dwd.radolan_rv`, certificate
  `dwd.rv.accumulation-to-interval-mean-rate.v1`: sampled via het officiële
  DE1200-randcertificaat;
- `radar.knmi.rtcor`, certificate
  `knmi.rtcor.accumulation-to-interval-mean-rate.v1`: expliciet
  `grid_semantics_withheld`;
- `radar.metoffice.uk_rate`, certificate `met-office.odim-rate.v1`: sampled via
  de artifact-exacte `AnalysisGrid`;
- `satellite.noaa.err`, certificate `noaa.err.instantaneous-rate.v1`: sampled via
  het exacte GLB-5-venster.

NOAA ERR is in de acquisitiecatalogus een satellietworkload, maar levert een
kwantitatief precipitation-rateveld en gebruikt daarom hetzelfde neutrale
targetbijdragecontract. De output blijft de oorspronkelijke workloadidentiteit
`satellite.noaa.err` dragen; de module hernoemt de bron niet tot radar.

## Targetsampling

Voor ieder beschikbaar target en iedere bron met bewezen gridranden wordt
WGS84-lengte/breedte met `always_xy=True` naar de gebonden native CRS
getransformeerd. De sampler gebruikt de gecertificeerde native bbox en de
conventie:

- west- en noordrand inbegrepen;
- oost- en zuidrand uitgesloten;
- rijen noord naar zuid en kolommen west naar oost;
- exact één containing pixel, zonder interpolatie of resampling.

De geometrieautoriteit is per bron expliciet:

- DWD vereist als input het vooraf uitgegeven officiële DE1200-gridcertificaat.
  De consumer herberekent de candidate-binding en controleert exact 1100 x 1200
  pixels, de officiële CRS en de officiële 1 km-randen;
- Met Office moet een artifact-exact `AnalysisGrid` dragen die gelijk is aan
  native CRS, dimensies en coverage;
- NOAA moet een exact 0,02-gradenvenster dragen dat op de bekende GLB-5 globale
  gridcentra en -grenzen uitgelijnd is;
- KNMI wordt niet met een bbox-verhouding gesampled. De bestaande RTCOR-gridgate
  is onvoorwaardelijk withheld en verbiedt dus een targetpixel en targetrate.

De KNMI-status is geen ontbrekende provider. De contribution bewaart de volledige
candidate-, certificate-, raster- en QC-contenthash, maar zet row, column en rate
op `null` en gebruikt `explicit_no_target_pixel_fail_closed_v1` als methode-ID.
De concrete open gridblockers zijn:

- conflicterende officiële CRS-authorities;
- niet formeel gedefinieerde `LU`-pixelrandsemantiek;
- de HDF 3.6 versus NL25/HDF 3.8 profielkloof;
- geen retained officiële live-HDF-gridbytes;
- een display-origin die niet aan de decoder is gebonden;
- geen gecertificeerde kilometer-naar-meter-CRS-equivalentie.

Een onbeschikbaar DesiredState-target wordt niet op zijn last-known positie
gesampled. Er verschijnt wel een contribution met `target_unavailable`, zodat de
bron-targetcombinatie nooit stil verdwijnt. Ook die status gebruikt de expliciete
fail-closed no-sample methode-ID.

## Meteorologische toestanden en fail-closed gedrag

- Geldige rate `> 0`: `positive_rain_candidate`, alleen een positieve
  `current_rain`-candidate.
- Geldige rate `== 0`: `dry_withheld`, negatieve claim geblokkeerd.
- Provider-undetect: `dry_withheld`, negatieve claim geblokkeerd en geen
  verzonnen `0.0`.
- Nodata: `nodata_withheld`, geen operationele claim.
- Gridranden niet bewezen: `grid_semantics_withheld`, geen pixel, rate of claim.
- Buiten native domein: `outside_domain`, geen operationele claim.
- Target unavailable: `target_unavailable`, geen operationele claim.

Elke contribution draagt `full_scan_bound=false` en
`external_truth_bound=false`. Targetsampling alleen kan dus nooit `current_dry`,
`no_threat` of een andere negatieve meteorologische waarheid publiceren. Daarvoor
zijn later een expliciet volledig scancontract en onafhankelijk extern
waarheidsbewijs nodig. Ook de positieve output is slechts een candidate; de
operationele factlaag moet nog haar eigen admission receipts en certificaten
binden.

## Coverage, QC en provenance

Iedere contribution bewaart:

- provider, workload en product;
- product-validiteitstijd en expliciet meegegeven worker-ontvangsttijd;
- target-ID en de door DesiredState gehashte positie;
- sample row/column of een expliciete reden waarom die ontbreken;
- `rain_rate_mm_h` wanneer de native pixel een exact kwantitatief getal draagt;
- provider-native `quality_state`;
- voor KNMI de contentgebonden native quality-index en geldigheidsmaskers, maar
  geen target-QC-scalar zolang er geen betrouwbare targetpixel bestaat;
- voor NOAA de native DQF-waarde en bijbehorende bitmaskers;
- source-, process- en semantics-certificate-SHA-256;
- canonical run/commit/receipt/result, capture receipt/manifest, artifact,
  artifactmetadata en native grid/coverage hashes.

De NOAA-target-QC-scalars en de KNMI-QC-rasterevidence worden alleen behouden.
Ze worden niet vertaald naar een zelfverzonnen confidence of quality score. DWD
en Met Office blijven expliciet `quality unavailable` omdat hun gecertificeerde
paden geen toepasbaar native QC-veld leveren.

Contribution- en batch-ID zijn rechtstreeks afgeleid van canonieke JSON en
SHA-256. De inputvolgorde van providers verandert de output niet; de batch wordt
gesorteerd op workload en daarna target-ID. Een dubbele provider/workload wordt
geweigerd, omdat de laag niet willekeurig één frame mag kiezen.

## Contract voor de multi-source cycle

De publieke invoer en uitvoer zijn:

```python
source = VerifiedRadarFieldSource(
    field=canonical_candidate,
    received_at=...,
    native_grid_certificate=dwd_certificate_or_none,
)
batch = build_radar_target_contribution_batch(desired_state, (source, ...))
```

`RadarTargetContributionBatch` is geschikt als content-addressed evidence in de
cyclelaag:

- schema: `stv3-multi-source-radar-target-batch-v1`;
- role: bijvoorbeeld `target_candidate_batch`;
- quantity/unit: precipitation rate / `mm/h`;
- sample count: `batch.sample_count`;
- valid sample count: `batch.valid_sample_count`;
- content hash: `batch.content_sha256`;
- content size: `batch.size_bytes`;
- receipt/processreferentie: `batch.process_receipt_sha256`.

`batch.canonical_bytes` zijn de onveranderlijke canonieke body/content-bytes die
exact door `batch.content_sha256` worden geïdentificeerd. Dit zijn bewust niet de
bytes van de volledige `as_dict()`-envelope met afgeleide IDs, tellers en receipt.
Een `EvidenceReference.size_bytes` moet daarom `batch.size_bytes` gebruiken.

De batch-processreceipt bindt het batchcontract, de batchcontenthash, de
DesiredState-hash, alle gesorteerde target-processhashes en per workload de unieke
candidate-content-, canonical-receipt-, geometry-, source- en
semantics-certificate-hashes. De cycle gebruikt dus nooit willekeurig de receipt
van één target.

Outside-domain, unavailable, nodata, undetect en grid-semantics-withheld blijven
in het batchdocument staan. Cycle-applicability op basis van catalogusland/bbox
mag die toestanden duiden, maar mag ze niet herschrijven tot een meteorologische
nul.

## Aanvullende radarworkloads die nog geen `mm/h` mogen leveren

De overige radarworkloads zijn niet vergeten. Ze mogen pas aan dit contract
worden toegevoegd nadat ze dezelfde decoder-, artifact-, canonical-evidence- en
semantics-certificategrens bezitten:

- `radar.aemet.composite`: gesloten kwantitatieve decoder, native grid/QC en
  rate-semantics certificate;
- `radar.dpc.sri`: gesloten SRI-decoder, unit/tijdsemantiek,
  artifactauthority en semantics certificate;
- `radar.kmi.appv4_visual`: geen visuele kleurinterpretatie; eerst officiële
  quantity/legend en kwantitatieve decoder/certificaat;
- `radar.kmi.public_wcs_unknown_quantity`: officiële
  quantity/unit/productsemantiek en daarna decoder/certificaat;
- `radar.kmi.radqpe_native`: eerst extern leveringscontract/credentialauthority,
  daarna decoder/artifact/certificaat;
- `radar.meteofrance.dpradar`: gesloten kwantitatieve decoder, native grid/QC en
  rate-semantics certificate;
- `radar.opera.dbzh`: reflectiviteit vereist een operationeel onderbouwde
  Z-R/conversie- en calibratiegate plus rate-semantics certificate;
- `radar.rainviewer.visual`: visuele tiles vereisen eerst officiële
  kwantitatieve productsemantiek en decoder/certificaat.

Tot die gates bestaan, moeten deze workloads in de gezamenlijke cycle als een
expliciete providerstatus verschijnen (`blocked`, `unavailable`, `visual_only` of
`semantics_not_certified`). Ze stil weglaten of een kleur/DBZH zonder toegelaten
conversie als `mm/h` presenteren is verboden.

## Tests

`tests/test_worker_multi_source_radar_facts.py` dekt:

- de exacte vier-provider allowlist;
- één deterministische multi-providerbatch over beschikbare en onbeschikbare
  targets;
- echte rasterbytebinding voor iedere candidate en een batch-processreceipt;
- DWD-, NOAA- en Met Office-targetsampling plus expliciete KNMI-grid-withholding;
- positieve rate, zero/dry, undetect, nodata en buiten-domein;
- half-open gridranden;
- native QC-retentie;
- semantics-, process- en quality-tampering;
- grid-asdrift en ontbrekende verplichte analysis-grid;
- dubbele providers, inputordening en ontvangsttijd.
