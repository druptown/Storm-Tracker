# Multi-source satellite target facts

## Wat deze laag werkelijk levert

`storm_tracker_worker.multi_source_satellite_facts` maakt één deterministische,
content-addressed matrix met **exact vier satellite-workloads × alle expliciete
DesiredState-targets**. Geen bron of target mag stil verdwijnen.

Deze laag doet geen netwerk- of bestands-I/O, start geen service, publiceert
geen operationele snapshot en maakt geen forecast, waarschuwing of admission-
beslissing. Ze projecteert uitsluitend reeds geverifieerd bewijs naar
provider-neutrale targetbijdragen.

| Workload | Bestaand geverifieerd bewijs | Targetresultaat nu |
|---|---|---|
| `satellite.goes18.rrqpe` | duurzame `RawManifestAuditOutputReceipt` over exact bewaarde listing/NetCDF-container | `quantitative_decoder_withheld` binnen catalogus-bbox; buiten bbox en unavailable blijven expliciet |
| `satellite.goes19.rrqpe` | duurzame `RawManifestAuditOutputReceipt` over exact bewaarde listing/NetCDF-container | idem |
| `satellite.hsaf.h40b` | duurzame `RawManifestAuditOutputReceipt` over exact bewaarde listing/NetCDF-gzip-container | idem |
| `satellite.noaa.err` | `NativeRainRateFieldCandidate` met NOAA-semanticscertificaat, exacte GLB-5-windowgeometrie, array-contenthash en bewaarde DQF-vlakken | echte target-rain-rate kandidaat, of expliciet zero/undetect/nodata/outside/unavailable |

Dat drie bronnen nog `withheld` zijn is geen ontbrekende rij. Het is het
gecontroleerde resultaat van hun huidige native contract: GOES en H40B
valideren container en metadata, maar lezen expliciet geen pixel- of
coördinaatwaarden. Een rain-rate, accumulatie, droge toestand of pixelpositie
voor die bronnen invullen zou dus bewijs verzinnen.

## Gesloten authorities

De workloadallowlist en catalogusapplicability zijn gesloten op acquisition-
catalogushash
`5f224f9aea2d5d77446a8df5b08dcaa44478611184fb312a7c5849399b9c4e6b`.
De drie raw-profielen binden exact workload, provider, product, audit-consumer,
platform-ID (waar aanwezig) en catalogus-bbox. De bbox zegt uitsluitend dat een
target volgens de catalogus relevant kan zijn; ze beweert geen native
pixeldekking of geolocatie.

Voor iedere raw receipt wordt zonder store-I/O opnieuw bewezen:

- canonieke outputbytes en `output_sha256`;
- outputreceipt en hard-isolated processreceipt;
- workload/consumer/stream, provider/product en DesiredState-hash;
- capturemanifest/capturereceipt;
- alle raw rollen met contenthash, receipthash en bytegrootte;
- `raw_bytes_verified=true` en alle decode/fact/admission/publicatieclaims
  exact `false`;
- de inerte procesboundary en effect-contracthash.

NOAA ERR gebruikt het bestaande targetpad uit
`multi_source_radar_facts`: certificate
`noaa.err.instantaneous-rate.v1`, `native_candidate_content_sha256`, de exact
uitgelijnde 0,02° GLB-5-window, half-open native pixelranden en de bewaarde
`dqf_*`/`quality_optimal_mask`-vlakken. De targetwaarde komt dus uit echte,
content-bound rasterbytes en niet uit catalogusmetadata.

## Betekenis van een bijdrage

Elke `SatelliteTargetContribution` bevat onder andere:

- DesiredState- en targetidentiteit;
- provider, workload, product en platform waar bewezen;
- `product_valid_at` en `received_at` waar beschikbaar;
- quantity/unit, `rain_rate_mm_h` of `accumulation_mm` alleen wanneer bewezen;
- expliciete observation-, coverage- en claimstatus;
- targetpixel en native QC-samples alleen wanneer werkelijk gesampled;
- catalogusapplicability met profielauthorityhash;
- source content-, receipt- en processhashes plus gesorteerde bewijsrollen;
- een targetgebonden process-evidencehash en eigen contentadres.

Voor NOAA mag alleen een strikt positieve, geldige pixel
`positive_rain_candidate` worden. Een geldige nul of undetect wordt
`dry_withheld`: een losse targetpixel bindt geen full scan en geen externe
waarheid. Nodata, buiten native domain en unavailable geven geen
meteorologische claim. Voor de drie raw-only bronnen blijven quantity, unit,
producttijd, pixel, rain rate en accumulatie `null`.

## Batch en cycle-adapter

`SatelliteTargetContributionBatch` bevat altijd precies vier bronnen en een
volledige, uniek gesorteerde workload×target-matrix. De publieke
`canonical_bytes` zijn exact de bodybytes waarop `content_sha256` slaat, niet de
volledige `as_dict()`-envelope. `size_bytes == len(canonical_bytes)`.

De batch-level `process_receipt_sha256` bindt:

- batchcontract en batchcontenthash;
- DesiredState-authority;
- de vier catalogus/source/receipt authorities;
- iedere gesorteerde targetgebonden process-evidencehash.

`to_cycle_evidence_reference()` levert een immutable, veld-voor-veld
cycle-compatibele referentie:

```text
role          = target_satellite_candidate_batch
schema_id     = stv3-multi-source-satellite-target-batch-v1
content_sha256= batch.content_sha256
receipt_sha256= batch.process_receipt_sha256
size_bytes    = len(batch.canonical_bytes)
```

Dit is kandidaatbewijs. De adapter verleent geen admission en maakt geen
`SatelliteInput`-claim over withheld bronnen.

## Nog vereiste decoder-/semantics-gates

Voor GOES-18 en GOES-19 RRQPE zijn minimaal nog nodig:

1. een begrensde decoder die RRQPE- en DQF-pixelarrays uit de geverifieerde
   NetCDF-bytes leest;
2. bewijs voor scale/offset/fill/undetect en de kwantitatieve unit;
3. een formele native scan-angle/geostationaire pixel-edge-naar-WGS84 mapping;
4. een gecertificeerd DQF-acceptatiebeleid zonder stil kwaliteitsverlies;
5. live golden-product- en tamperfixtures voor beide satellieten.

Voor H SAF H40B zijn minimaal nog nodig:

1. een geautoriseerd live golden-product (de bestaande contractfixture bevat
   geen weerpixels);
2. begrensd lezen van `rr` en `qind`, inclusief packing/missing-bewijs;
3. formele geostationaire FCI pixel-edge/projectiemapping naar targets;
4. gecertificeerde interpretatie van QIND en parallax-correction;
5. expliciete temporele semantics van de nominale sleuf voordat rate of
   accumulatie wordt geclaimd.

Voor een negatieve operationele NOAA-claim blijven bovendien een volledige
scanauthority en onafhankelijke externe waarheid nodig. De bewaarde DQF-codes
zijn bewijs, maar deze laag promoveert ze niet zelfstandig tot een
meteorologisch kwaliteitsbeleid.

## Tests

`tests/test_worker_multi_source_satellite_facts.py` bewijst onder meer:

- exacte vier-workloadallowlist tegen de acquisition-catalogus;
- volledige en deterministische 4×targets-matrix;
- echte NOAA-rastertargetwaarde, DQF-retentie en fail-closed
  zero/undetect/nodata/outside;
- expliciete GOES/H40B withheld-, bbox- en unavailable-statussen;
- raw document/role/receipt/claim-tamper;
- NOAA certificate/QC/grid-tamper;
- duplicate/omission-, target- en evidence-rolegrenzen;
- contribution-/batchcontentadressen en cycle EvidenceReference-projectie;
- afwezigheid van I/O-, service-, forecast- en alertwiring.
