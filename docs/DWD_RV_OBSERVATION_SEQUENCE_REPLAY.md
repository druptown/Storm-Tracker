# DWD RV officiële +000-observatiereplay

Status: lokale, content-addressed en volledig ongewirede science-inputkandidaat.
De implementatie staat in
`storm_tracker_worker/dwd_rv_observation_replay.py` en
`storm_tracker_worker/dwd_rv_science_sequence.py`. Ze wordt niet geïmporteerd
door `service.py`, de autonome scheduler, config, Home Assistant, MQTT,
snapshotpublicatie of admissioncode.

## Bron en gesloten observatiebeleid

De vaste broncorpus `.analysis/dwd-rv-observation-sequence-20260807/` bevat 36
officiële DWD `composite_rv_YYYYMMDD_HHMM.tar`-archieven van 2026-08-07 05:10
tot 08:05 UTC, exact om de vijf minuten. De broninventaris bindt voor ieder
archief naam, bytes, SHA-256, basistijd, +000-member en de afzonderlijk
aanwezige forecastleads +005 tot en met +120. Alleen +000 mag een scienceframe
worden. Ieder ander `lead_seconds` faalt gesloten; forecastleden worden nooit
met latere observaties gemengd.

De bronmanifestauthority heet
`stv3-dwd-rv-observation-source-manifest-v1`. Ze bindt 36 unieke oplopende
basistijden, 300 seconden cadence en 41.072.640 exacte raw bytes. De loader
verifieert canonical JSON, file-SHA, filegrootte, alle frame-SHA's, alle rollen,
leadaccounting en totaalgrootte vóór een sequencemanifest kan worden gesloten.
Het vaste bronmanifest is 15.681 bytes met SHA-256
`20335e6c50b35d57dd111bea7e3670f515e933da5239ba39e74ccebc41030852`.
Er vindt geen netwerkrefetch plaats.

## Keten per basistijd

Iedere frame gaat serieel door dezelfde bestaande stores en authorities:

```text
exact officiële tarbytes
  -> raw-v2 source receipt + capture
  -> versieerbare DWD raw-v2/quantitative identity-handoff
  -> producerreceipt vóór lifecycle-ready
  -> één lifecyclelease
  -> verse hard-isolated DWD child
  -> +000 quantitative decoder
  -> canonical radar evidence
  -> officieel DWD-gridcertificaat
  -> vaste 1-km België-analysis-grid
  -> lossless hierarchical cells-shadow
  -> durable output- en processreceipt
  -> onafhankelijke outputauthority-verificatie
  -> enige lifecycle-ack
  -> bestaande ShadowEvidenceFrame-algebra
```

De analysis-grid is bronuitgelijnd, 316 bij 316 cellen (99.856 pixels), met
DWD's officiële stereografische CRS, oorsprong `(-500, -449500)` meter en
1.000 meter resolutie. Resolutie is sampling support en wordt niet als
meetnauwkeurigheid voorgesteld. Detector-, transform- en pipelineconfiguratie
zijn volledig gehasht. De export bewaart de volledige genormaliseerde raster,
de gebonden `HierarchicalDetectionFrame` en exhaustieve pixelaccounting; hij
maakt geen tweede rasterformaat.

## Content-addressed sciencecontract

Per +000-frame wordt één immutable
`stv3-science-shadow-dwd-evidence-record-v1` geschreven. Dat record bindt:

- canonical run-, commit-, receipt- en result-SHA;
- capture-ID, capture-receipt en manifest-SHA;
- native candidate-ID, content- en lineage-SHA plus radarsemantiekcertificaat;
- analysis-field-ID, content- en lineage-SHA;
- transformcertificaat, transformreceipt, transformconfig en target-grid-SHA;
- detector- en pipelineconfig-SHA;
- volledige `NormalizedRasterFrame` met alleen finite waarden of `null`;
- volledige hiërarchische detectie en exhaustieve pixelaccounting;
- alle bestaande operational-admissionblockers;
- event-, producer-, desired-authority-, isolation-, output- en
  processreceipt-SHA's van de completion.

Het geordende manifestcontract is exact
`stv3-science-shadow-canonical-sequence-v1`. De sequence-ID is de SHA-256 van
contract + bronmanifest-SHA + alle geordende canonical run-ID's en
evidence-content-SHA's. Alle 36 entries hebben `lead_seconds=0`, dezelfde
target-grid-SHA en exact 300 seconden cadence. `forecast_generated`,
`motion_skill_claimed` en `meteorological_admission_granted` blijven `false`.
Een reeks frames is dus input voor latere motionvalidatie, geen motion-
skillbewijs op zichzelf.

## Procesmetingen en lagere cap

Een apart content-addressed
`stv3-dwd-rv-observation-process-metrics-v1`-manifest bindt per verse child:

- framevolgnummer, valid_at, event- en output-SHA;
- parent walltime;
- exacte child CPU-seconden;
- werkelijke peak RSS;
- peak van de platformspecifieke kernel-memory-boundary.

Het manifest berekent p50, p95 en max via de expliciete nearest-rankmethode en
bindt isolation-authority, isolation-limits, bronmanifest en science-sequence.
De memory-boundarycounter moet strikt onder de geconfigureerde cap blijven.
RSS en boundarycounter worden bewust apart gehouden: Windows begrenst process
memory charge en Linux `RLIMIT_AS`; dat zijn niet dezelfde grootheden als
resident memory.

### Gemeten volledige corpusrun

De exclusieve Windows-run van alle 36 frames eindigde met exitcode 0, 36 unieke
outputreceipts, 36 unieke processreceipts, 36 consumptionreceipts, 36 canonical
decoder-runs, nul `ready`/`leased` restitems en nul actieve decoderchildren. Er
waren geen `busy`, deadline-, CPU-, memory-, protocol- of datafailures.

Een verse calibration-child onder de 1,5-GiB-vangrail mat 711.565.312 bytes
boundary charge en 234.221.568 bytes echte peak RSS. De beproefde lagere grens
is daarom als volgt afgeleid: calibration-boundarymax + vaste 256 MiB marge,
omhoog afgerond op de volgende 64 MiB = 960 MiB (1.006.632.960 bytes). Dat is
37,5% lager dan 1,5 GiB.

Alle 36 children slaagden vervolgens onder 960 MiB. Nearest-rankstatistieken:

| Grootheid | p50 | p95 | maximum |
|---|---:|---:|---:|
| child walltime | 11,7323 s | 13,9400 s | 14,7930 s |
| child CPU | 11,3594 s | 13,6406 s | 14,4688 s |
| echte peak RSS | 223,55 MiB | 223,84 MiB | 223,89 MiB |
| boundary memory | 678,84 MiB | 679,42 MiB | 679,55 MiB |

De hoogste boundarywaarde was 712.560.640 bytes: 294.072.320 bytes (280,45
MiB) onder de 960-MiB-cap. Dit bewijst de cap voor exact deze 36 officiële
archieven en dit profiel op de testhost; het is geen universele live-NUC-SLO.
De 1,5-GiB-default blijft alleen een vangrail en wordt niet als acceptabel
normaalverbruik voorgesteld.

Content-addressed resultaten:

- bronmanifest SHA-256:
  `20335e6c50b35d57dd111bea7e3670f515e933da5239ba39e74ccebc41030852`;
- science sequence-ID:
  `0fc414def8b41ddd27c433a73875d625ea40f5b485a2d330c6edd654c3848dc2`;
- sequencemanifest SHA-256:
  `e94b8f14393fb0e2076fd54ff7b0a2c0133416cd1cde87bb18acd01df5eda48a`;
- processmetricsmanifest SHA-256:
  `7e501592a64faf9bece7af064a425a2d7a3f96719ae90847f7b9753a9a308b60`.

De export bevat 36 immutable evidenceobjects (18.299.803 bytes) en drie
manifestobjects (53.733 bytes); alle 39 bestandsnamen kwamen exact overeen met
hun opnieuw berekende SHA-256.

## Platformdurability en resterende blockers

Op de Linux-worker behouden lifecycle- en rawstores hun echte fsync/CAS-
durability. De lokale Windows-exportharnas gebruikt uitsluitend voor zijn
offline lifecyclemap een expliciete directory-fsync-no-op, omdat de store
generiek Windows-directory-fsync bewust niet claimt. Content-SHA's,
SQLite-transacties en immutable exportobjects blijven geverifieerd, maar een
power-loss tijdens deze lokale Windows-analyserun kan heruitvoering vereisen.
Dit is geen live workerclaim.

Nog expliciet geblokkeerd:

- geen `service.py`-, scheduler-, MQTT-, HA-, snapshot- of live wiring;
- geen operational admission of factrevision;
- geen weerclaim uit één frame en geen motion-skillclaim uit alleen deze reeks;
- geen live-gevalideerde NUC-cap; de corpusrun bewijst alleen bounded gedrag
  voor deze 36 officiële archieven op de testhost;
- KMI RADQPE, OPERA en alle niet-DWD-routes blijven buiten dit contract.
