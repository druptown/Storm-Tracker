# DWD RV kwantitatieve decodergrens

## Status en afbakening

`storm_tracker_worker/dwd_rv_quantitative.py` implementeert een geïsoleerde,
fail-closed decodeketen voor exact één reeds voltooide schema-3-capture van het
DWD-product RV. De keten is nog niet gekoppeld aan `service.py`, `radar.py`,
Home Assistant of enige operationele consument. De providerregistry blijft
ongewijzigd op `vnext_operational_admission = false`.

Een succesvolle decode is daardoor uitsluitend een **niet-operationele
kandidaat** met controleerbare provenance. Ze is geen toelatingsbesluit en geen
bewijs dat radar op zichzelf de volledige meteorologische waarheid weergeeft.
RV is een radarafgeleide neerslagaccumulatie; vergelijking met onafhankelijke
grondwaarnemingen en held-out gebeurtenissen blijft nodig vóór operationele
toelating.

## Expliciete aanroep- en importgrens

Een import van `storm_tracker_worker.dwd_rv_quantitative` is inert. De module
maakt geen store of standaardpad aan, opent geen SQLite-database of
netwerkverbinding, start geen thread en importeert geen `service`, `radar` of
providerregistry. Alleen een expliciete aanroep van `decode_dwd_rv_capture()`
met door de aanroeper geconstrueerde absolute stores, een capture-ID, een
decoder-job-ID en een attempt-ID kan de offline kandidaatketen uitvoeren.

Deze grens wordt in een schoon subprocess getest terwijl socketverbindingen,
SQLite-connecties en threadstarts verboden zijn en de tijdelijke werkmap voor
en na import identiek moet blijven. Daarmee is deze decoder nog geen live
handler, scheduler, providerregistratie of operationele admission-route.

## Primaire specificatiebronnen

De decoder gebruikt alleen semantiek die uit primaire bron aantoonbaar is:

1. De [DWD-productfiche RV in HDF5](https://www.dwd.de/DE/leistungen/radarprodukte/radarkomposit_rv_hdf5.pdf?__blob=publicationFile&v=3)
   noemt RV een over vijf minuten geaccumuleerde neerslaghoeveelheid in mm,
   beschikbaar per vijf minuten, op een raster van 1200 × 1100 pixels met een
   horizontale resolutie van 1 km × 1 km en een stapgrootte van 0,001 mm.
2. De [officiële DWD-mededeling over de HDF5-omschakeling](https://www.dwd.de/DE/leistungen/opendata/neuigkeiten/opendata_august2025_1.html)
   documenteert de RV-tarstructuur en de ledennaam
   `composite_rv_yyyymmdd_HHMM_PPP-hd5`, waarbij `PPP` de
   voorspellingsstap is.
3. Het door DWD aangewezen [EUMETNET OPERA ODIM H5 2.3-model](https://www.eumetnet.eu/wp-content/uploads/2019/01/ODIM_H5_v23.pdf)
   definieert onder meer `COMP`, de cartesiaanse `where`-attributen,
   `quantity_value = offset + gain × raw_value`, de afzonderlijke betekenis van
   `nodata` en `undetect`, en `ACRR` als geaccumuleerde neerslag in mm.
4. De officiële DWD Open Data-capture
   `composite_rv_20260801_2305.tar`, lid
   `composite_rv_20260801_2305_000-hd5`, is als actuele
   compatibiliteitsproef gebruikt. Ze bevatte ODIM 2.3, `COMP`, `ACRR`, het
   hieronder vastgelegde WGS84-stereografische raster en dezelfde vijf minuten
   tussen `starttime` en `endtime`.

De productfiche specificeert niet ieder intern attribuut letterlijk. Daarom is
het actuele, daadwerkelijk door DWD geleverde profiel streng vastgelegd. Als
DWD bijvoorbeeld CRS, interne productnaam of encodering wijzigt, wordt de
capture `unsupported` in plaats van stilzwijgend opnieuw geïnterpreteerd. Een
profielwijziging vereist eerst nieuwe primaire documentatie en certificatie.

## Invoercontract

De enige toegestane invoer is
`RawArtifactStore.load_capture_bundle_bytes()` voor de rol `rv_tar`. Die
primitive verifieert capturemanifest, receipt en exacte tarbytes met een
volledige objectread/SHA-pass. De onafhankelijke canonical-evidencecommit
verifieert de raw authority daarna nog een keer. Er wordt geen pad uit metadata
geopend en er bestaat geen fallback naar een legacy-resultaat.

De capture moet exact voldoen aan één van twee gesloten captureprofielen:

- het bestaande `worker-dwd-rv-exact-raw-v1`, uitsluitend behouden voor
  historische replays; of
- `worker-dwd-rv-observation-handoff-v1`, uitsluitend geproduceerd door de
  versieerbare raw-v2-adapter uit
  `storm_tracker_worker/dwd_rv_capture_handoff.py`.

Het tweede profiel bindt de oorspronkelijke `provider=dwd` raw-v2 capture,
receipt- en manifesthashes, de onveranderde archivehash, `+000` als enige
observatie en iedere forecast `+005..+120` afzonderlijk. De decoder valideert
het volledige adaptercontract opnieuw vóór selectie van `+000`; checks zijn
daarmee uitgebreid met een tweede exacte allowlistregel, niet versoepeld.

Beide captures moeten verder exact voldoen aan:

- schema-3, volledig en cryptografisch geldig volgens `RawArtifactStore`;
- provider `dwd_radolan`;
- captureproduct `dwd_open_data_composite_rv_capture`;
- exact één capturelid met `member_id == role == rv_tar`;
- memberproduct `dwd_open_data_composite_rv_tar`;
- ondersteund tar-contenttype en maximaal 8 MiB;
- capturelid en raw receipt hebben identieke receipt-ID, ontvangsttijd,
  content-SHA-256 en receipt-SHA-256;
- de collectorprovenance benoemt native bytes vóór decode, de vijf minuten,
  de eenheid mm en forecast lead 0, maar claimt nog geen geverifieerde
  kwantitatieve decode;
- zowel raw upstream-ID als capture upstream-ID worden opnieuw opgebouwd uit
  product, geselecteerde member, producttijd en content-SHA-256 en moeten byte
  voor byte overeenkomen.

Zie `docs/DWD_RV_CAPTURE_HANDOFF.md` voor het append-only source→targetcontract,
de vaste echte tarfixture en de expliciete scheiding tussen observatie en
forecast.

Een beschadigd raw object stopt in de `RawArtifactStore`-integriteitscontrole.
In dat geval wordt zelfs geen canonieke foutreceipt geschreven, omdat de raw
autoriteit niet langer bewijsbaar is.

## Veilige selectie van het HDF5-lid

De tar wordt alleen sequentieel gelezen (`r:`) en nooit uitgepakt op het
bestandssysteem. De grenzen zijn 64 leden en 40 MiB per HDF5-lid.

Ieder tarlid moet:

- een gewoon bestand en direct child zijn, zonder `/`, `\` of traversal;
- exact aan `composite_rv_YYYYMMDD_HHMM_PPP-hd5` voldoen;
- dezelfde basistijd dragen;
- een lead van 0 tot 120 minuten in stappen van 5 minuten dragen;
- een unieke naam hebben.

Er moet exact één lid met `PPP = 000` zijn. De naam levert de eerste
onafhankelijke producttijd. Geen forecastlid wordt gedecodeerd of gemengd.

## HDF5/ODIM/RV-validatie

De decoder accepteert alleen het gecertificeerde profiel:

- `Conventions = ODIM_H5/V2_3`;
- `/what/object = COMP`, `/what/version = H5rad 2.3`;
- DWD-bronidentificatie `ORG:78` en `CTY:616`;
- precies één `dataset1` en precies één `dataset1/data1`;
- `/dataset1/what/product = MAX` en `prodname = RV_top_view`;
- `/dataset1/data1/what/quantity = ACRR`;
- een gewone, niet-virtuele en niet-externe `uint32`-dataset;
- `gain ≈ 0.001`, `offset = -gain`, `nodata = 4294967295` en
  `undetect = 0`;
- niet gesimuleerd indien `how/simulated` aanwezig is;
- top-level tijd = intervaleinde = tijd in het `+000`-lid =
  schema-3 `product_timestamp`;
- intervaleinde minus intervalstart exact 300 seconden.

Er worden geen ontbrekende waarden aangevuld en geen onbekende eenheden,
quantities of tijdsbetekenissen afgeleid.

## Raster- en CRS-contract

Het native raster blijft intact:

- arrayvorm `(1200, 1100)`, dus asvolgorde `yx`;
- 1100 kolommen en 1200 rijen;
- 1000 m × 1000 m pixels;
- rijen lopen van noord naar zuid;
- polaire stereografische projectie met `lat_ts=60`, `lat_0=90`, `lon_0=10`,
  de door DWD geleverde false easting/northing en de WGS84-ellipsoïdeparameters;
- alle vier ODIM-hoeken (`LL`, `LR`, `UL`, `UR`) zijn verplicht.

De vier WGS84-hoeken worden met PROJ terug naar de native CRS getransformeerd.
Ze moeten binnen 0,25 m een rechthoek van exact 1100 km × 1200 km vormen. Voor
het gecertificeerde profiel is de native bounding box:

```text
minimum_x =      -500 m
maximum_x =   1 099 500 m
minimum_y = -1 199 500 m
maximum_y =        500 m
```

Zowel de volledige griddefinitie als de coverage krijgen een afzonderlijke
SHA-256. Geen vaste geografische bbox of geïmproviseerde pixelorigin wordt als
fallback gebruikt.

## Behoud van waarden en maskers

Het afgeleide raster bevat vijf afzonderlijke arrays in native resolutie:

1. `raw_values`, little-endian `uint32`: alle oorspronkelijke HDF5-pixelcodes,
   inclusief sentinels;
2. `accumulation_mm`, little-endian `float64`: uitsluitend voor geldige pixels
   de officiële lineaire omzetting; op ongeldige pixels staat een technisch
   vulgetal 0,0 dat nooit zonder masker mag worden gebruikt;
3. `valid_mask`, `uint8`;
4. `nodata_mask`, `uint8`: nooit bestraald/geen data volgens ODIM;
5. `undetect_mask`, `uint8`: wel bestraald maar onder de detectiegrens volgens
   ODIM.

De drie maskers zijn wederzijds uitsluitend en bedekken samen iedere pixel.
`undetect` wordt dus niet als gemeten 0 mm samengevoegd met geldige nulwaarden,
en `nodata` wordt nooit als droog weer geïnterpreteerd.

Er vindt aan deze grens nadrukkelijk geen omzetting naar mm/u, drempeling,
celvorming, objecttracking of toekenning van een kwaliteitsgetal plaats.

## Deterministisch immutable artifact

Het formaat `stv3-worker-dwd-rv-quantitative-raster-v1` bestaat uit:

- vaste magic bytes en een big-endian headerlengte;
- canonieke UTF-8 JSON-metadata;
- de vijf arrays in vaste volgorde;
- deterministische zlib-compressie op niveau 6.

De metadata bindt minimaal:

- capture-ID, capture-receipt-SHA en manifest-SHA;
- archive-SHA, geselecteerde membernaam en member-SHA;
- decoderidentiteit, versie en config-SHA;
- quantity, eenheid, vijfminuteninterval, gain, offset en sentinels;
- CRS, grid, coverage en alle sectie-offsets, datatypes, lengtes en SHA's;
- SHA en lengte van zowel de ongecomprimeerde als gecomprimeerde arraypayload.

De SHA-256 van het volledige artifact bepaalt de bestandsnaam en het opaque
handle `artifact:dwd-rv-quantitative:<sha256>`. Publicatie gebeurt via een
volledig geschreven en `fsync`'d stagingbestand en een exclusieve hardlink. Een
bestaand contentpad wordt nooit overschreven en moet volledig overeenkomen.
Objectgrootte, totale bytes, objectaantal (standaard maximaal 4096) en
stagingaantal (standaard maximaal 8) zijn afzonderlijk hard begrensd. Alleen
exacte content-addressed object- en stagingnamen zijn toegelaten. Capaciteitsmeting,
staging, hardlink en eindverificatie vallen onder één thread- én OS-bestandslock,
zodat twee processen dezelfde store niet kunnen overschrijven of overboeken.
Zowel de objectmap als het opruimen van staging wordt duurzaam met `fsync`
vastgelegd. Er is geen retentie, eviction of degradatie naar een kleiner raster
in deze module.

De eerste opener publiceert daarnaast atomair `.store-profile.json`. Dat
canonieke profiel bindt de artifactversie, suffix en alle byte-, object- en
staginglimieten blijvend aan de store-root. Publicatie gebruikt een volledig
geschreven en `fsync`'d stagingbestand, een exclusieve hardlink en `fsync` van
staging en root; een onderbreking vóór de link laat geen profiel na en een
onderbreking erna wordt bij retry exact geadopteerd. Een tweede instantie met
andere limieten faalt gesloten, ook als ze dezelfde OS-lock gebruikt. Iedere
publicatie scant de begrensde layout vóór zowel een nieuw object als een exacte
retry; een retry kan dus geen onbekende entry of reeds overschreden store
omzeilen.

De bytegrens is de eenmaal-per-inode getelde logische `st_size` van objecten en
staging, niet een claim over werkelijk toegewezen filesystemblokken, compressie
of vrije schijfruimte. Een filesystemquota en capaciteitsmonitor blijven dus een
aparte operationele gate. Ook de beschreven `fsync`-volgorde is codebewijs van
de bedoelde commitgrenzen, geen vervanging voor de echte NUC/filesystem-
power-lossmatrix.

De OS-lock wordt non-blocking gepolld onder dezelfde monotone decoderdeadline.
Ook wachten op de lokale threadlock en op een ander proces eindigt daardoor
fail-closed zodra de jobdeadline verstrijkt. Zonder expliciete decodecheckpoint
geldt voor directe store-aanroepen dezelfde begrensde standaardduur van 60
seconden. Dit voorkomt een oneindige lockwait; de parent-enforced harde
subprocessdeadline blijft daarnaast verplicht.

De decode-entrypoint draagt daarnaast één absolute monotone coöperatieve
deadline van standaard 60 seconden door de raw-load, tarselectie, HDF-decode,
artifactpublicatie, herverificatie en canonical commit. Iedere zware grens wordt
erna opnieuw gecontroleerd. Dit voorkomt dat een reeds verstreken job nog
bewijs commit, maar onderbreekt geen vastgelopen native HDF5-/zlib-aanroep. Een
parent-enforced subprocessdeadline met CPU/RSS-isolatie blijft daarom een harde
admissiongate.

## Commitvolgorde en foutuitkomsten

De succesvolgorde is strikt:

1. schema-3 capture, raw receipt en tarbytes in een authority-pass verifiëren;
2. de al geverifieerde immutable tarbytes begrensd inspecteren;
3. exact één veilig `+000`-lid selecteren en hashen;
4. HDF5-, RV-, tijd-, waarde-, grid- en CRS-contract valideren;
5. native arrays en drie maskers bouwen;
6. immutable artifact volledig publiceren;
7. artifact opnieuw lezen en alle interne hashes en array-invarianten
   verifiëren;
8. pas daarna `CanonicalEvidenceStore.commit(... decoded_complete ...)`.

De canonieke verwijzing heet uitdrukkelijk
`non_operational_candidate_artifact`. Het canonical evidence-contract en de
providerregistry geven daarmee geen impliciete operationele toelating.

Bij een bekende afwijzing wordt alleen een payloadvrije canonieke receipt met
`unsupported`, `incomplete` of `decode_error` geschreven. Alle
meteorologische velden, inclusief quantity, unit, CRS, grid, coverage en
artifactreferentie, zijn dan `null`. Een publicatiefout laat geen
`decoded_complete`-receipt achter. Een niet-bestaande of cryptografisch
onbetrouwbare capture kan helemaal geen canonieke uitkomst krijgen.

## Testdekking

`tests/test_worker_dwd_rv_quantitative.py` dekt de geldige productieachtige
keten en onder meer:

- deterministische retry en immutable hergebruik;
- afzonderlijke valid/nodata/undetect-semantiek;
- afwijkende quantity, product, object, gain, offset en sentinels;
- fout vijfminuteninterval of verschil tussen capture/member/HDF5-tijd;
- gewijzigd CRS, vervormde hoeken, gesimuleerde data en meerdere datasets;
- corrupte tar, traversalnaam en dubbele `+000`;
- verkeerde capturerollen;
- raw-objecttampering en derived-artifacttampering;
- foutinjectie vóór artifactpublicatie;
- harde object- en stagingaantallen, onbekende layoutentries en atomische
  parallelle publicatie;
- immutable storeprofielen, mismatch tussen instanties, onderbreking vóór/na
  profielpublicatie, exacte-retry-layoutcontrole en deadlinegebonden
  cross-process lockwait;
- deadline vóór decode en na exact één raw-authoritypass, zonder artifact of
  canonical evidence;
- expliciete controle dat de DWD-providerregistry operationeel gesloten blijft.

Gerichte validatie na deze storehardening: de DWD-suite gaf `34 passed`, de
gedeelde echte-proces lock/profile-suite `6 passed`; de gecombineerde DWD-,
KNMI- en processuite gaf `113 passed`. Dit is functioneel regressiebewijs op de
huidige ontwikkelhost, geen power-loss- of NUC-soakbewijs.

De module kan pas aan live scheduling of een operationele radarconsument worden
gekoppeld na een afzonderlijke review van bronprofiel, resourcebelasting,
grondvalidatie, replayresultaten en admission policy.
