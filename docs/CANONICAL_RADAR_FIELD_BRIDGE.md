# Canonical radar-field bridge

Status: lokale, worker-only, shadow-only kandidaat; niet geregistreerd, niet
gepland, niet operationeel toegelaten, niet naar Home Assistant gepubliceerd en
niet gedeployed.

## Doel en harde grens

`storm_tracker_worker/canonical_radar_field_bridge.py` is de afzonderlijke brug
tussen twee reeds bestaande bewijsniveaus:

1. een `CanonicalEvidenceStore`-`DecoderRunReceipt` dat alleen hashes en exact
   decoder-/grid-/tijdmetadata bevat;
2. het checksumgebonden, content-addressed providerartefact met de werkelijk
   gedecodeerde native arrays.

De brug neemt alleen een `run_id` aan. Zij roept altijd
`CanonicalEvidenceStore.load(run_id)` aan. Daardoor worden het opgeslagen
receipt, zijn hash-/revisieketen en de volledige schema-3 raw capture opnieuw
geverifieerd; `RawArtifactStore.load_capture_receipt()` gaat via de volledige
capturebundle en leest/SHA-verifieert ieder raw lid. Een willekeurig
caller-supplied receipt kan deze grens niet omzeilen.

Daarna opent `ProviderDerivedArtifactAuthority` het exacte object dat door het
receipt wordt genoemd. Handle, SHA-256, objecttype, objectgrootte, intern
artefactschema, arraychecksums, providersemantiek en capturebinding worden via
de providerstore opnieuw geverifieerd. De brug vergelijkt vervolgens
onafhankelijk:

- provider, product, decoderidentity, decoderversie en config-SHA;
- capture-ID, capture-receipt-SHA en capture-manifest-SHA;
- quantity, unit, producttijd en volledige tijdondersteuning;
- native CRS, dimensies, grid-SHA, coverage-SHA en coveragegrenzen;
- expliciete valid/nodata/undetectmaskers en iedere behouden QC-plane.

Een afwijking levert uitsluitend een begrensde `RadarFieldIssue` op. Er wordt
geen gedeeltelijk veld, droogte, nulregen, fallbackprovider of admissiontoken
gemaakt.

## Exacte semanticscertificaten

De gesloten standaardallowlist bevat vier certificaten. Hun SHA-256-fingerprint
omvat ieder veld hieronder en verandert dus automatisch bij decoder- of
configdrift.

| Provider/product | Receiptsemantiek | Kandidaatsemantiek | Gridstatus | QC-status |
|---|---|---|---|---|
| KNMI `knmi_nl_rdr_data_rtcor_5m_capture` | exact 300 s gauge-adjusted accumulatie, `mm` | **intervalgemiddelde** `mm/h = mm * 3600 / 300`; nooit instantane rate | native NL25 behouden; nog geen `AnalysisGrid`, omdat de native proj4-as in km niet impliciet naar een metrische CRS wordt herschreven | native quality-index en adjustmentlagen volledig behouden; geen qualityscore en adjustment nooit opnieuw toegepast |
| NOAA ERR `noaa_err_listing_and_netcdf4_capture` | instantane satelliet-neerslagrate, `mm/h`, met productinputvenster | identiteit naar `instantaneous_precipitation_rate`; geen radar- of groundclaim | native EPSG:4326-regio behouden; geen impliciete metrische reprojicering | raw DQF, missing, bad, degraded en optimal afzonderlijk behouden; geen afgeleide confidence |
| Met Office UK ODIM `RATE` | nominale `surface_precipitation_rate_estimate`, `mm/h`, met afzonderlijke niet-uniforme input-start/eindtijd | identiteit naar dezelfde rate-estimate; het inputvenster blijft zichtbaar | het vaste tmerc-grid heeft expliciete metre-eenheid, bovenlinkse pixelbound en exact sluitende 1 km-bounds; daardoor bestaat een native, resamplingvrije `AnalysisGrid`-candidate | geen qualitydataset: status blijft expliciet `unavailable:odim_quality_dataset_absent` |
| DWD RV | exact 300 s accumulatie, `mm` | **intervalgemiddelde** `mm/h = mm * 3600 / 300`; nooit instantane rate | native RADOLAN-CRS/coverage behouden; geen `AnalysisGrid`, omdat de bbox uit geprojecteerde WGS84-hoeken met 0,25 m tolerantie komt en dus geen exact outer-edge-origin bewijst | geen native qualityplane; status blijft unavailable |

De primaire bronnen zijn onderdeel van ieder certificaat. Voor KNMI zijn dat de
officiële productpublicatie/HDF-semantiek; NOAA gebruikt de NESDIS STAR-
productbeschrijving; Met Office gebruikt de officiële AWS-productregistratie
plus ODIM 2.2 (`RATE = mm/h`); DWD gebruikt de officiële RV/RADOLAN-bronnen.

## Tijd is geen detail

De brug kent drie categorisch verschillende tijdprofielen:

- `instantaneous_rate_estimate` voor NOAA ERR;
- `interval_mean_rate_from_exact_accumulation` voor KNMI en DWD;
- `nominal_rate_with_nonuniform_input_window` voor Met Office.

Een 5-minutenaccumulatie wordt alleen omgerekend wanneer het artefact exact
300 seconden tussen `interval_start` en `interval_end` bewijst. Het resultaat
heet bewust `interval_mean_precipitation_rate`. Het wordt nooit hernoemd naar
een instantane meting. Bij 299 of 301 seconden wordt de kandidaat ingehouden.

De nominale/representatieve tijd moet exact aan de canonical producttijd zijn
gebonden en binnen het behouden input-/accumulatievenster liggen. Er wordt geen
ophaaltijd, afgeronde frame-index of veronderstelde cadans gebruikt.

## Nodata, undetect en echte nul

`NativeRainRateFieldCandidate` bevat afzonderlijk:

- de originele fysieke bronarray (`mm` of `mm/h`);
- het afgeleide rateveld in `mm/h`;
- `valid_mask`;
- `nodata_mask`;
- `undetect_mask`;
- alle providergebonden auxiliary/QC-planes.

De drie maskers moeten exhaustief en onderling consistent zijn. Nodata en
undetect mogen niet overlappen. In de kandidaatarray is iedere ongeldige waarde
`NaN`, nooit `0.0`; `value_or_none()` geeft daar `None`. Een echte, door de
provider geldige nul blijft wel exact `0.0`.

Het bestaande `MetricRasterField` gebruikt technisch een finite 0-fill plus
een autoritatief `valid_mask=False`. Daarom wordt zo'n tupleveld niet standaard
gematerialiseerd. Dat gebeurt alleen met een expliciete calleroptie, alleen
wanneer een exact resamplingvrij `AnalysisGrid` bestaat en alleen onder een
afzonderlijke kleine pixelgrens. Ook dan blijft het masker autoritatief en is
de technische nul nooit droog bewijs.

## Geen verborgen meteorologie

De kandidaat legt machineleesbaar vast:

```text
admission_status = candidate_only_unadmitted
reprojection_applied = false
resampling_applied = false
thresholding_applied = false
wet_dry_classification_applied = false
```

Er is dus nog geen cel-, systeem-, motion-, forecast-, groundconfirmation- of
waarschuwingsbeslissing. Provider-QC wordt bewaard maar niet tot een zelfbedacht
gewicht of confidencepercentage herleid.

OPERA `DBZH` is bovendien hard uitgesloten, zelfs wanneer een caller een
zelfgemaakt passend certificaat probeert mee te geven. Reflectiviteit is geen
rain rate. Zonder een afzonderlijk, product-/regimegebonden en wetenschappelijk
gevalideerd Z-R-certificaat mag die conversie niet bestaan.

## Resourcegrenzen en deadlines

De brug heeft onafhankelijke grenzen voor:

- maximaal 4.000.000 native pixels;
- maximaal 256 MiB gebonden arraybytes;
- maximaal 32 auxiliary planes en 32 semanticscertificaten;
- maximaal 1.000.000 pixels voor expliciete `MetricRasterField`-
  materialisatie;
- één absolute monotone bridge-deadline, standaard 90 seconden.

Providerstores houden daarnaast hun eigen object-, byte-, staging- en
decompressiegrenzen. Deadlinechecks staan vóór en na raw load, artefactread,
arrayvalidatie, conversie en materialisatie. Bestaande KNMI/DWD-storelezers
hebben intern nog geen preëmptieve checkpoint per read/decompressiestap. De
deadline is daar dus coöperatief; een parent-enforced hard child met CPU-, RSS-
en wallclocklimiet blijft verplicht vóór shadow scheduling of live gebruik.

## Testbewijs

`tests/test_worker_canonical_radar_field_bridge.py` dekt onder meer:

- raw-herverificatie vóór iedere artefactread en fail-closed raw tampering;
- direct-rate en exact 300 s accumulation-to-interval-mean-rate;
- weigering van een afwijkend interval;
- config-/semanticscertificaatdrift vóór artefact-I/O;
- harde OPERA/DBZH-uitsluiting;
- capture-, decoder-, grid-, coverage- en producttijdbinding;
- exhaustieve maskers, echte nul versus nodata en behouden native QC;
- optionele kleine `MetricRasterField` met autoritatief nodatamask;
- artefactfout, verlopen deadline, rastercapaciteit en
  materialisatiecapaciteit;
- gesloten standaardcertificaatinventaris zonder OPERA.

De repositorybrede kandidaat-isolatietest behandelt de brug samen met de
kwantitatieve decoders als een inert shadowoppervlak: geen productiehandler,
service, providerpolicy of HA-module mag haar importeren.

Gerichte validatie tijdens implementatie:

```text
tests/test_worker_canonical_radar_field_bridge.py
tests/test_worker_quantitative_candidate_isolation.py
19 passed, ResourceWarning-as-error
```

## Open poorten

Deze brug sluit alleen de technische, raw-/artefactgebonden native-fieldgrens.
Nog open en afzonderlijk vereist:

1. hard child CPU/RSS/wallclock-isolatie rond iedere providerartefactread;
2. onafhankelijk held-out meteorologisch bewijs per provider, seizoen,
   intensiteit, geografie en QC-stratum;
3. expliciete, gevalideerde reprojection/resampling naar één gekozen
   vergelijkingsgrid waar native grids niet exact samenvallen;
4. kwantificering van reprojection-, beam-, compositing-, satelliet- en
   representativiteitsonzekerheid;
5. producttijd-high-watermarks, cadence/gapbeleid en multi-frame selectie;
6. objectdetectie-/tracking-/nowcastinput die de maskers en tijdsemantiek
   zonder verlies consumeert;
7. NUC-load-, thermische en HA/Z2M-latencysoak;
8. shadowreplay, canary, expliciete admissionreview, rollback en pas daarna
   eventuele wiring.

Tot al die poorten groen zijn, is `decoded_complete` plus deze bridgecandidate
nog steeds geen bewijs van operationele betrouwbaarheid.
