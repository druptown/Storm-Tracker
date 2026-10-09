# DPC SRI exact-native capturekandidaat

Status: volledig geisoleerde worker-only kandidaat, lokaal getest, niet
gescheduled, niet gewired, niet gedeployed en niet operationeel toegelaten.

## Officiele bron en product

De kandidaat gebruikt uitsluitend de officiele Italiaanse Dipartimento della
Protezione Civile-bronnen:

- [Piattaforma Radar-DPC](https://mappe.protezionecivile.gov.it/it/mappe-e-dashboard-rischi/piattaforma-radar/);
- [Radar-DPC v2 REST-API](https://dpc-radar.readthedocs.io/it/latest/api.html);
- [webservice- en productdocumentatie](https://dpc-radar.readthedocs.io/it/latest/clientwss.html).

De officiele documentatie meldt dat de vernieuwde API sinds 12 januari 2026
actief is. `SRI` betekent *Surface Rainfall Intensity*: een vijfminutenproduct
van het nationale radarcomposiet, met regenmeterinformatie verwerkt, in
`mm/h`. Het oorspronkelijke SRI-product is een geprojecteerde GeoTIFF van
`1200 x 1400` Float32-pixels, nominaal 1 km, met LZW-compressie. Dat is
productspecificatie; de kandidaat leest nog geen waarden en promoveert dus
geen pixel tot een kwantitatief bewijsfeit.

De API-volgorde is:

1. `GET /findLastProductByType?type=SRI` met de officiele web-origin;
2. selecteer exact het ene `SRI`-head met `period=PT5M` en integer milliseconden;
3. `POST /downloadProduct` met precies `productType` en `productDate`;
4. valideer het begrensde antwoord en zijn tijdelijke S3-URL;
5. download het geselecteerde oorspronkelijke GeoTIFF-object.

De DPC-publicatie waarschuwt tevens voor ontbrekende radarcoverage,
kwaliteitsverschillen en mogelijke anomalieen. Een geldig bestand is daarom
niet automatisch een juiste lokale meting en zeker niet automatisch een
nat-/droogbesluit.

## Eerlijke schema-3-memberset

De complete capture heeft exact twee upstream-native rollen:

- `catalog_json`: de ongewijzigde bytes van het officiele headantwoord;
- `geotiff`: de ongewijzigde bytes van het geselecteerde SRI-object.

`downloadProduct` levert een korte AWS presigned URL met een security token.
Die query is een vluchtige transportcredential en geen meteorologisch
bronobject. Ze wordt strikt in het geheugen gecontroleerd maar nooit in een
artifact, receipt, capture, resultaat, fixture of document geschreven.

Een aanvankelijk overwogen derde, afgeleid `download_identity_json`-member is
bewust verworpen. `RawArtifactStore` schema 3 aanvaardt alleen echte
upstream-native bytes; afgeleide JSON als native labelen zou de bewijslaag
vervalsen. De geheimvrije selectie-identiteit wordt daarom deterministisch in
de provenance van het exacte GeoTIFF-receipt gebonden:

- producttype en catalogus-producttijd;
- vaste bucket, objecthost, key en exact pad;
- begrensde vervaltijd uit het transportantwoord;
- expliciet `presigned_url_persisted=false` en
  `presigned_query_persisted=false`.

De capture-ID bindt producttijd, key en SHA-256 van beide exacte members. Een
reopen kan de selectie daardoor zonder credential of netwerk reconstrueren.

## Harde transport- en inputgrenzen

`storm_tracker_worker/dpc_native.py` past fail-closed grenzen toe:

- absoluut capturebudget: maximaal 60 monotone seconden, inclusief thread- en
  filelock;
- I/O-timeout per HTTP-operatie: maximaal 15 seconden en nooit langer dan het
  resterende capturebudget;
- catalogus maximaal 2 MiB, downloadantwoord 64 KiB, GeoTIFF 25 MiB;
- verplichte, exact overeenkomende `Content-Length`;
- alleen identity transportencoding;
- vaste contenttypes per endpoint;
- HTTP 200, geen redirects en `response.geturl()` moet exact gelijk blijven;
- JSON: UTF-8 strict, dubbele sleutels verboden, maximaal 4096 nodes en diepte
  16;
- signed URL: uitsluitend HTTPS naar
  `s3-prod-dpc-radar.s3.eu-south-1.amazonaws.com`, poort 443, exact objectpad,
  geen userinfo/fragment, maximaal 8192 tekens en een gesloten queryveldprofiel;
- catalogustijd tussen 2000 en 2100 en exact op een vijfminutengrens;
- objectkey exact `SRI/DD-MM-YYYY-HH-MM.tif` en tijdgebonden aan de catalogus;
- TIFF maximaal 256 IFD-entries, verplichte raster- en GeoTIFF-tags en het
  waargenomen `1200 x 1400`, 32-bit, LZW, een sample, Float-profiel.

De jobpayload moet exact leeg zijn. Een caller kan dus geen URL, token,
bestandskey, geografisch gebied of selectie injecteren.

## Durable-before-parse en retries

De exacte catalogusbytes worden eerst atomair opgeslagen en pas daarna als
JSON geinspecteerd. De exacte GeoTIFF-bytes worden eveneens eerst opgeslagen
en pas daarna wordt de begrensde TIFF-directory bekeken. De inspector decodeert
geen strip/tile en leest geen rasterpixel.

Een complete retry gebruikt `load_capture_bundle_bytes`, controleert receipt,
hash, memberset, tijd en container opnieuw en doet geen netwerkrequest. Een
partiele retry gedraagt zich als volgt:

- na durable catalogus: catalogus wordt hergebruikt; downloadtransport en
  GeoTIFF worden opnieuw verkregen;
- na gevalideerde maar nog niet opgeslagen tijdelijke URL: catalogus wordt
  hergebruikt en een nieuwe korte URL wordt aangevraagd;
- na durable GeoTIFF maar voor capturecommit: beide exacte members plus de
  receiptgebonden selectie-identiteit worden offline hergebruikt;
- na capturecommit: de complete capture is gezaghebbend, ook als het proces
  onmiddellijk daarna wegviel.

Threadconcurrentie en procesconcurrentie worden geserialiseerd onder dezelfde
deadline. Een capaciteitsfout of corruptie maakt nooit een gedeeltelijke
capture compleet en wordt nooit vertaald naar `droog`, `mist` of een lagere
kwaliteit.

## Live read-only profiel

Een kleine live controle op 2 augustus 2026 rond 05:39 UTC zag:

- catalogushead `SRI`, `1785648600000`, `PT5M`;
- object `SRI/02-08-2026-05-30.tif`;
- 385.126 bytes, ETag `23300b18b47a941478a64eed513a3998`;
- SHA-256
  `02b4cafe0c2a689a41ff36b0f6c444bb6171983d76db07688ea17889e1590e7a`;
- little-endian TIFF, IFD-offset 373.312, 24 entries;
- 1200 x 1400, 32 bit, LZW, een sample, Float;
- GeoTIFF-tags 33550, 33922 en 34735.

Alleen dit kleine identity/profile staat in
`tests/fixtures/dpc_sri_v2_live_identity_profile.json`. Het grote object en de
presigned URL staan niet in git.

## Wat uitdrukkelijk nog niet bewezen is

Deze kandidaat bewijst transport-, object- en containeridentiteit. Hij bewijst
niet:

- de exacte GeoTIFF-projectie, assenvolgorde, tiepoint, pixelschaal en extent;
- nodata/undetect/negative- en anomaliewaarden;
- dat iedere Float32-pixel werkelijk bruikbaar `mm/h` is;
- radar-/regenmeter-QC, coverage of lokale representativiteit;
- nat/droog, cellen, systemen, beweging, snelheid of voorspelling;
- geschiktheid voor enige trackerlocatie;
- operationele admission.

Dit is extra belangrijk omdat de bestaande legacycode een handgeschreven LCC-
CRS gebruikt, terwijl de actuele officiele productdocumentatie een eigen
Transverse-Mercatorprofiel beschrijft. De kandidaat neemt geen van beide als
waarheid aan: de latere kwantitatieve decoder moet de CRS en GeoTIFF-keys tegen
de officiele productspecificatie en echte golden bytes bewijzen.

## Isolatie- en testbewijs

`tests/test_worker_dpc_native_capture.py` bevat 27 gerichte tests voor:

- exacte members, SHA-identiteit, durable-before-inspect en complete retry;
- vier injecteerbare partiele foutgrenzen en een fout na commit;
- vijf abrupte subprocesscrashes met offline herstel waar mogelijk;
- raw-store-capaciteit en geen incomplete capture;
- twee gelijktijdige callers met een fetch;
- verlopen deadline en deadlinegebonden threadlock;
- redirects, verkeerde endpoint-URL, encoding, contenttype, status en lengte;
- signed-URL SSRF, host/path/query/signature/fragment en key/tijd drift;
- duplicate JSON keys en gesloten catalogusschema;
- TIFF-profiel zonder pixelbibliotheek of pixelread;
- een kleine echte live identity/profile-fixture;
- lege jobpayload en secretvrije artifacts/resultaten;
- afwezigheid uit `PRODUCTION_JOB_HANDLERS` en alle Home Assistant-importen.

De auditregistry bevat uitsluitend de kandidaatstring
`storm_tracker_worker.dpc_native:capture_latest_dpc_sri` met required roles
`catalog_json` en `geotiff`. `capture_status` blijft
`legacy_artifact_only`, `exact_native_bytes_complete=false`,
`vnext_operational_admission=false` en de actieve servicehandler blijft
`storm_tracker_worker.provider_radar:dpc_radar_fetch`.

## Volgende gesloten gates

Voor enige wiring zijn minstens nodig:

1. een afzonderlijke harde decoder in een resource-begrensd childproces;
2. exacte GeoKey-/tiepoint-/scale-/extent-/axisvalidatie tegen de officiele
   DPC-productspecificatie;
3. Float32/LZW-decode met begrensde allocatie en expliciete nodata/QC-regels;
4. afgeleide facts cryptografisch binden aan capture en decoderversie;
5. onafhankelijke Italiaanse held-out regen- en coverage-events;
6. thermische, CPU-, RSS-, deadline-, disk-full- en langdurige NUC-soaktests;
7. worker-snapshotcutover waarbij Home Assistant uitsluitend compacte
   resultaten projecteert.

Tot al die gates groen zijn, verandert deze kandidaat niets aan live execution.
