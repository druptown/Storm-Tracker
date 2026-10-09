# OPERA CIRRUS regionale DBZH/QI-decoder v1

Status: **strikt ongewirede, niet-operationele kandidaat**. De implementatie
staat in `storm_tracker_worker/opera_dbzh_quantitative.py`. Zij is niet
geregistreerd als worker-handler, staat niet in een providerregistry of
scheduler en wijzigt geen Home Assistant-, service- of deployconfiguratie.

De kandidaat decodeert een begrensd regionaal venster uit een reeds volledig
gecertificeerde OPERA-native v3-capture. `decoded_complete` betekent alleen dat
een structureel en semantisch intern consistente derived kandidaat bestaat.
Het is geen meteorologische admissionreceipt en geen neerslagbesluit.

## Productbetekenis

OPERA CIRRUS is een pan-Europees composiet van de **maximum horizontal
reflectivity factor** (`DBZH`), niet van rain-rate of accumulatie. Het
[officiele productblad](https://www.eumetnet.eu/wp-content/uploads/2024/06/OPERA_Max-Reflectivity_Product-Sheet_Ed-2.0.pdf)
beschrijft een raster van 4.400 bij 3.800 pixels op 1 km resolutie, elke vijf
minuten, met een afzonderlijk product-quality-indexraster.

Volgens ODIM geldt voor een fysische waarde:

```text
physical = offset + gain * raw
```

In het huidige exacte CIRRUS-contract zijn gain en offset voor DBZH en QI
respectievelijk `1.0` en `0.0`. De decoder past die formule toch expliciet toe
op gedetecteerde DBZH-pixels en bewaart daarnaast altijd de oorspronkelijke
float64 bronwaarden.

`nodata=-9999000.0` betekent geen brondata. `undetect=-8888000.0` betekent dat
wel werd gemeten maar dat het signaal onder de detectiegrens lag. Deze twee
toestanden worden nooit samengevoegd. Een gewone negatieve dBZ-waarde is niet
automatisch undetect.

QI loopt voor geldige waarden van 0 tot en met 1. **QI=0 is een geldige, zeer
lage kwaliteit en is niet missing.** Alleen de exacte QI-nodatacode is missing.
De decoder gebruikt QI niet als filter en verzint geen kansinterpretatie.

## Verplichte native v3-authoriteit

De decoder accepteert uitsluitend:

- schema-3 `RawArtifactStore`;
- provider `opera`;
- captureproduct `eumetnet_opera_cirrus_dbzh_capture`;
- exact een capturelid met rol `hdf5`;
- collector `worker-opera-cirrus-dbzh-exact-raw-v3`;
- inspector `opera-cirrus-live-golden-contract-v3`;
- certificatie `opera-cirrus-certification-v3`;
- een aanwezig immutable `validated_complete` v3-verdict;
- volledig overeenkomende raw receipt-, capture-, revision-, object-key- en
  tijdidentiteiten.

Een v2-receipt, ontbrekend v3-verdict, afwijkende collector of gewijzigde
capture-identiteit wordt niet stilzwijgend gerecertificeerd. Raw-integriteits-
of SHA-fouten leveren helemaal geen canonical decoderresultaat op.

## Een raw-pass per lid en job

`OperaDbzhRawAuthority` is een een-capture-sessie. De eerste
`load_capture_bundle_bytes()` doet de enige schema-3 payloadread en SHA-pass en
houdt uitsluitend immutable receiptviews en bytes in de jobsessie bij.

De `CanonicalEvidenceStore` moet via `open_opera_evidence_store()` aan exact
dezelfde authority-sessie worden gebonden. De evidence-store valideert daarna
zelfstandig capture-receipt, manifest, leden en alle hashes, maar krijgt de
reeds geverifieerde payload-free captureview terug. Daardoor ontstaat geen
tweede HDF5-read/SHA-pass. Een nieuwe proces-/jobsessie doet opnieuw maximaal
een raw-verificatie; een retry binnen dezelfde sessie doet er nul extra.

Een evidence-store die aan een andere authority is gebonden wordt voor de
decode geweigerd.

## WGS84-bbox naar het exacte LAEA-raster

De invoer is een niet-wrappende WGS84-bbox met strikt stijgende latitude en
longitude. Antimeridiaanwrapping, niet-eindige waarden, nuloppervlak en punten
buiten het OPERA-domein worden voor raw access geweigerd.

De vier geografische hoeken alleen zijn onvoldoende voor een niet-lineaire
projectie. Daarom samplet de decoder ieder van de vier bboxranden met 256
segmenten: 1.028 begrensde punten. Alle punten worden met `always_xy=true` naar
de exacte CIRRUS-LAEA-projectie getransformeerd:

```text
+proj=laea +lat_0=55.0 +lon_0=10.0 +x_0=1950000.0
+y_0=-2100000.0 +units=m +ellps=WGS84
```

Na de gedensifieerde projectie wordt rondom 2.000 meter conservatieve marge
toegevoegd. Het venster wordt vervolgens naar buiten afgerond op gehele
bronpixels en aan het native domein begrensd.

Voor globale zero-based rij `r` en kolom `c`:

```text
x(c) = (c + 0.5) * 1000
y(r) = -(r + 0.5) * 1000
```

Rij 0 is dus de noordelijke rij en kolom 0 de westelijke kolom. Het artifact
legt zowel de oorspronkelijke WGS84-aanvraag als globale row/column offsets,
de geselecteerde LAEA-bbox, densificatie, marge en hashes van grid en coverage
vast.

Harde venstergrenzen:

- maximaal 1.024 rijen;
- maximaal 1.024 kolommen;
- maximaal 1.048.576 pixels;
- elke overschrijding faalt voor HDF5-rasteraccess.

## Opnieuw gecontroleerd HDF5-contract

Naast het v3-verdict valideert de decoder voor zijn eigen grens opnieuw:

- de exacte rootboom zonder extra objecten;
- ODIM 2.4, H5rad 2.4, ORG:247 en `COMP/MAX/DBZH`;
- nominale tijd en het 599-seconden invoervenster;
- `/how` creator, publisher, rechten, comment en canonieke nodeshash;
- de exacte LAEA-projectie, schalen, dimensies en hoekattributen;
- DBZH gain/offset/nodata/undetect;
- QI gain/offset/nodata en task `pl.imgw.quality.qi_total`;
- `CLASS=IMAGE` en `IMAGE_VERSION=1.2` op beide rasters;
- little-endian float64, chunkvorm 760 bij 880 en gzip-9;
- alle fysieke chunks, filtermasks, unieke objecttokens en globale
  DBZH/QI-byte-intervaldisjunctie;
- uitsluitend interne hard links en geen externe/virtuele opslag.

Pas daarna worden uitsluitend de geselecteerde DBZH- en QI-slices gelezen.

## Exacte derived arrays

Een geslaagde kandidaat bevat negen arrays met dezelfde begrensde `yx`-shape:

1. `dbzh_raw`, `<f8`: oorspronkelijke DBZH inclusief speciale codes;
2. `detected_dbz`, `<f8`: fysieke dBZ op detected pixels, elders exact nul;
3. `dbzh_nodata_mask`, `uint8`;
4. `dbzh_undetect_mask`, `uint8`;
5. `dbzh_detected_mask`, `uint8`;
6. `coverage_mask`, `uint8`;
7. `qi_raw`, `<f8`: oorspronkelijke QI inclusief nodata;
8. `qi_valid_mask`, `uint8`;
9. `qi_missing_mask`, `uint8`.

Een nul buiten `detected_dbz` heeft zonder `dbzh_detected_mask` geen betekenis.
De parser reconstrueert alle masks uit de raw arrays en vergelijkt ze exact.
Hij controleert QI 0..1 alleen waar `qi_valid_mask=1`, accepteert QI=0, en
verwerpt niet-eindige detected DBZH of geldige QI.

Er wordt bewust geen gelijkheid tussen QI-valid en DBZH-detected afgedwongen:
de twee maskfamilies blijven onafhankelijke bronfeiten.

## Immutable artifact en decoderverdict

Het derived artifact heeft contract
`stv3-worker-opera-dbzh-qi-region-v1`. Het bestaat uit:

- vaste magic en begrensde canonieke JSON-header;
- exacte capture-, receipt-, revision-, rechten- en v3-verdictprovenance;
- producttijd en volledig invoervenster;
- WGS84-aanvraag, native crop, grid- en coveragehashes;
- per array dtype, shape, raw bytecount, raw SHA-256, zlib-9 bytecount en
  compressed SHA-256;
- uitsluitend maskaantallen, zonder weersclassificatie.

Publicatie is content-addressed en create-if-absent via tempbestand, fsync,
hardlink en directory-fsync. De store verifieert het gepubliceerde object en
parseert alle arrays opnieuw voor canonical evidence wordt toegevoegd. Een
bestaande hash met afwijkende bytes of metadata faalt gesloten.

De durable decoderverdictcache bindt capture-receipthash, member receipt- en
contenthash, decoderconfig, WGS84-regio en native windowhash. Verdicts zijn
canonieke immutable JSON-objecten van maximaal 64 KiB:

- `decoded_complete` verwijst naar exact een opnieuw verifieerbaar artifact;
- `deterministic_rejection` bewaart alleen outcome en begrensde reason code.

Een deterministic data-/contractafkeur wordt bij retry niet opnieuw zwaar
geinspecteerd. Een success-verdict wordt voor de canonical evidencecommit
gepubliceerd, zodat een commitcrash geen tweede HDF5-inspectie vereist.

Deadline-, echte I/O-, raw-integriteits-, capacity- en publicatiefouten worden
niet als deterministische bronverdict gecachet.

## Resourcegrenzen

- absolute coöperatieve decoderdeadline: 120 seconden;
- decoderbronobject: maximaal 32 MiB;
- uncompressed arrayset: maximaal 32 MiB;
- accounted peak working set: maximaal 128 MiB;
- immutable artifact: maximaal 40 MiB;
- artifactstore: maximaal 512 MiB en 4.096 objecten;
- staging: maximaal 80 MiB en acht objecten;
- verdictstore: maximaal 64 MiB en 65.536 verdicts.

Capacity accounting en publicatie zijn binnen een deadlinegebonden
cross-process OS-filelock geserialiseerd. Vreemde bestanden, symlinks,
overgrote stagingobjecten of een volle store worden geweigerd; er is geen
evictie of truncatie.

## Canonical evidence is nog geen admission

Alleen na artifact-publicatie, SHA-controle, volledige herparse en vergelijking
met de actuele raw-authoriteit wordt een `decoded_complete` run aan
`CanonicalEvidenceStore` toegevoegd. Die run publiceert uitsluitend:

- quantity `maximum_horizontal_reflectivity_factor`;
- unit `dBZ`;
- exacte native CRS, regionale grid- en coveragehash;
- een opaque `artifact:opera-dbzh-quantitative:<sha256>` handle;
- expliciet een `non_operational_candidate_artifact`.

Het artifact verklaart:

- `rain_rate_conversion_applied=false`;
- `hydrometeor_classification_applied=false`;
- `upstream_advected_contributions=true`;
- `motion_truth_eligible=false`;
- `operational_admission_eligible=false`.

Er bestaat geen dBZ-naar-mm/u-conversie, neerslagstatus, ground confirmation,
motionvector, forecast of nat/droogclaim.

## Verificatie op 2 augustus 2026

De actuele, afzonderlijk uitgevoerde bewijssuites geven:

- regionale kwantitatieve decoder: `23 passed`;
- OPERA native v3-capture: `107 passed`;
- canonical evidence store: `50 passed`;
- schema-3 raw-artifactstore: `37 passed, 1 skipped`;
- kandidaat-isolatie: `4 passed`;
- samen: `221 passed, 1 skipped`;
- `py_compile` voor decoder en testmodule: geslaagd.

De gerichte decodersuite dekt:

- float64 bronbehoud en alle maskinvarianten;
- QI=0 als geldige kwaliteit;
- onafhankelijkheid van DBZH-detected en QI-valid;
- densificatie, projectieguard, outward rounding en pre-raw capacity reject;
- exact een raw-verificatie en geen tweede inspectie op retry;
- duurzame success- en deterministic-rejectionverdicts over herstart/crash;
- geen caching van deadline of I/O;
- raw-corruptie zonder artifact/evidence;
- oude collector, ontbrekend v3-verdict en niet-eindige pixels;
- artifactcapacity, trailing bytes en objecttampering;
- kandidaat-isolatie van handlers, policies en Home Assistant;
- replay tegen de echte read-only golden.

Voor golden object
`80cb81b9c953a8fac9bcb8127a342aed0f93d0f203764caf8866fd0d244b471a`
werd bbox `50.90..51.00 N, 4.25..4.40 E` geselecteerd. De conservatieve crop
was 17 bij 17 pixels, globale rijen 2526..2542 en kolommen 1543..1559. Alle
289 pixels waren in dit ene tijdslot `undetect`; QI was daar missing. Dat is
correct als twee afzonderlijke bronmaskers bewaard en **niet** als droogte
geclassificeerd.

## Blokkades voor operationele wiring

Minimaal open blijven:

1. expliciete OPERA-rechten- en attributiebevestiging;
2. onafhankelijke redteamreview van decoder, artifact en authority-sessie;
3. distributiegeschikte echte fixtures met vaste hashes en meerdere
   meteorologische regimes;
4. event-held-out vergelijking met nationale radar en onafhankelijke
   grondwaarnemingen;
5. gevalideerde QI-policy zonder QI=0 als missing te behandelen;
6. alleen indien nodig: een apart wetenschappelijk gecertificeerd
   reflectiviteit-neerslagmodel met fase/climatologie/onzekerheid;
7. NUC memory/thermal soak en globale worker-capacityplanning;
8. een afzonderlijke immutable operationele admissionreceipt;
9. pas daarna expliciete registry-, scheduler-, service- en HA-wijzigingen.
