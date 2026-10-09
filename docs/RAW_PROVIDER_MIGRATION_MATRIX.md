# Raw-provider migratiematrix

Status: worker-only live op NAS-HA sinds 31 augustus 2026; meteorologische
admission blijft gesloten.

Deze lijst beschrijft de legacy-live/raw-captureregistry en is machineleesbaar
gespiegeld in `storm_tracker_worker/provider_capture_registry.py`. Dat is niet
dezelfde telling als de actuele acquisition-activationmatrix: die bevat 28
cadenceworkloads, waarvan 27 via de live worker in shadow-raw uitvoerbaar zijn
en alleen `radar.kmi.radqpe_native` hard geblokkeerd blijft. Blitzortung is
daarnaast één afzonderlijk, nog geblokkeerd event-ingresscontract en geen
cadenceworkload. De live raw-kalibratie-index bevat op het controlepunt van
31 augustus 2026 28 captures, 11 bronnen en 9 providers. Dat is live
opslagbewijs, geen operationele meteorologische toelating. Een bestaand gzip-JSON-
artifact met gedecodeerde observations of overlays is **geen** originele
providerpayload. `vNext operational admission` blijft daarom gesloten totdat
de exacte upstreambytes, de volledige memberset, producttijd, receipttijd en
productspecifieke semantiek aantoonbaar zijn.

Let op: de detailrijen hieronder bewaren de technische migratiehistoriek en
kunnen nog formuleringen als `niet gewired` of `legacy blijft actief` bevatten.
Voor de actuele uitvoerbaarheid van raw capture zijn de activationmatrix en de
live workerstatus leidend; voor meteorologische admission blijven de harde
gates in iedere rij leidend.

Een afzonderlijke immutable offline corpusauthority bewaart inmiddels echte
NOAA ERR-, METAR/AWC- en GOES-18/19-providerbytes content-addressed en verwijst
hashgebonden naar de bestaande DWD-, publieke KMI-WCS- en Houstonreplays. Het
canonieke manifest heeft SHA-256
`99500ab54f19196c0115176f31115039ec9b992d3e34b54235e91d726e06d5e9`.
Dit verandert geen activatiestatus: KNMI RTCOR en Met Office RATE hebben lokaal
geen retained officiële HDF5-bytes en hun synthetische fixtures blijven buiten
de echte-providerlane; publieke KMI WCS blijft structureel en nooit RADQPE.

Een afzonderlijk cutovermanifest heeft bovendien netwerkwerk gevonden dat
buiten deze workerjobregistry viel: MeteoLux, GeoSphere Austria en ItaliaMeteo
worden als validation/model-guidance vanuit de HA-providerlifecycle opgehaald,
en Blitzortung-push is eveneens nog HA-owned. Dat ze geen primaire radarclaim
publiceren maakt de belasting en raw-provenance niet onbelangrijk. Deze vier
paden zijn daarom expliciete `blocked_gap`-workloads; vóór cutover moeten ze
ofwel exact naar de worker migreren, ofwel volledig verdwijnen uit de runtime.

Twee bestaande providerpaden bereiken de exacte store: KMI appv4 en DWD RV.
Daarnaast bevat de gesloten registry twaalf geïsoleerde, geteste exact-byte
kandidaten: RainViewer, KNMI, Met Office, Météo-France, OPERA, NOAA ERR, GOES
RRQPE (G18/G19), H SAF H40B, DPC SRI, Synoptic, METAR en Netatmo.
Een kandidaat-handler is uitsluitend auditmetadata en geen servicewiring. Geen
enkele rij is operationeel toegelaten. KMI appv4 blijft categorische
visualisatie;
DWD bewaart de response-tar met echte `+000`-memberidentiteit en heeft een
schema-3-gebonden kwantitatieve decoderkandidaat, maar de legacy-afleiding en
vaste legacykwaliteit `0.98` zijn geen gecertificeerde feiten. Exacte raw
herkomst is noodzakelijk, maar promoveert een decoder nooit vanzelf.

Voor de drie grondnetwerken bestaan inmiddels eveneens afzonderlijke,
ongewirede schema-3-decoders. Ze schrijven uitsluitend laagste-niveau
station-/sensor-/tijd-/waarde-/QC-feiten naar de append-only
`CanonicalGroundEvidenceStore`; geen enkele decoder beslist zelf nat, droog,
grondbevestigd of operationeel toegelaten. De capture blijft bij iedere read de
cryptografische authority. Sitecrosswalk, ruimtelijke representativiteit,
fusion en echte held-out gebeurtenissen blijven aparte gates.

| Workerjob | Bron | Huidige rawstatus | vNext operationeel | Volgende harde gate |
|---|---|---|---|---|
| `kmi_fetch` | KMI appv4 | exact JSON + PNG, complete capture; publieke WCS-GeoTIFF is alleen structureel gevalideerd en niet als hoeveelheid gecertificeerd | nee; visualisation-only | geautoriseerde native KMI RADQPE GeoTIFF/ODIM-feed, productspecificatie en gebruiksrecht verkrijgen; nooit de publieke GRAY_INDEX-WCS als mm/u aannemen |
| `rainviewer_fetch` | RainViewer | legacy decoder blijft actief; geïsoleerde kandidaat bewaart exact manifest + 3×3 onverzachte radar-PNG's + 3×3 coverage-PNG's voor één bewijsbaar gedekte regio; 53 gerichte tests en publieke New York-golden groen; niet gewired | nee; categorische reflectiviteitsvisualisatie, één frame, kaartgeneratietijd is geen uniforme observatietijd | providerrecht/attributie + centrale lage-belastingplanning + duurzame tijdreeks (minstens 20 echte captures) + volledige kleur/pixel/coverage-decoder en onafhankelijke validatie; nooit als kwantitatieve rain-rate promoveren |
| `knmi_fetch` | KNMI | legacy WMS-pad blijft actief; geïsoleerde native `nl_rdr_data_rtcor_5m/1.0` HDF5-capture (60 gerichte tests) en schema-3-gebonden RTCOR-decoderkandidaat zijn groen op echte golden evidence; niet gewired | nee | onafhankelijke held-out kalibratie/grid/QC-validatie, harde child CPU/RSS/deadline- en NUC-soakgate, producttijd-high-watermark en snapshot-cutover |
| `dwd_radolan_fetch` | DWD RADOLAN/RADVOR RV | exact RV-response-tar, complete schema-3-capture met `rv_tar`; afzonderlijke schema-3-gebonden RV-decoderkandidaat bewaart kwantitatieve rasterfeiten. Een ongewired officieel DWD+ODIM-certificaat bindt nu bovendien 1100×1200, 1 km, arrayvolgorde en de exacte native pixelranden `west=-500`, `noord=500`, `oost=1099500`, `zuid=-1199500` aan dezelfde native CRS | nee; alleen de native-gridtransform is exact bewezen; legacy decoder/admission blijven dicht | echte multi-event unit/projectie/QC-validatie, harde child CPU/RSS/deadline en NUC-soak; daarna pas snapshotcutover |
| `odim_hdf5_fetch` | Met Office | legacy pad blijft actief; geïsoleerde kandidaat bewaart exact het vorige en actuele UTC-dagoverzicht uit S3 plus het geselecteerde oorspronkelijke UK 1 km ODIM `RATE`-HDF5-bestand. De structurele inspectie leest geen pixels, bewaart nominale producttijd en afzonderlijke input-start/eindtijd en is met 31 gerichte tests en een echte publieke golden groen; ODIM 2.2 bindt `RATE` aan `mm/h`; niet gewired | nee; exact raw, native identiteit en standaardeenheid zijn bewezen, maar geen pixelwaarde, provider-QC of operationele geschiktheid | afzonderlijke begrensde `RATE`-decoder, provider-specifieke nodata/undetect/QC- en compositinginterpretatie, onafhankelijke held-out UK-events, harde child CPU/RSS/deadline, NUC-soak en snapshotcutover |
| `opera_hdf5_fetch` | EUMETNET OPERA | legacy pad blijft actief; geïsoleerde exacte ODIM HDF5-capture (107 tests) en regionale DBZH/QI-decoder (23 tests) zijn ongewired en op een echte Belgische golden getest | nee; de golden had `undetect` DBZH en ontbrekende QI en levert dus expliciet geen rain-rate-/nat-/droogclaim | onafhankelijke kwantitatieve DBZH→R-certificatie, bron-/node-/QI-beleid, held-out events, harde child-isolatie en snapshotcutover |
| `aemet_radar_fetch` | AEMET | legacy decoder gebruikt een ongedocumenteerde webbundel en behandelt bytewaarden `1..8` als intensiteitsklassen; officiële audit bewijst uitsluitend nationale laagste-PPI-reflectiviteit in dBZ, EPSG:4326 en een per-file `ESCALA`-mapping | nee; legacy byteklassen zijn geen rain-rate of gecertificeerde reflectiviteitslegenda | overstappen op officiële OpenData `GET /api/red/radar/nacional`; envelope + exacte `datos`-response + exacte `metadatos`-response atomair schema-3 bewaren; officiële golden, tijdsupport, exact grid, `ESCALA`, NoData/no-echo/dekking en pixel-QC certificeren; api_key nooit bewaren |
| `dpc_radar_fetch` | DPC Italië | legacy decoder blijft actief; geïsoleerde Radar-DPC-v2-kandidaat bewaart exact het officiële SRI-headcatalogusantwoord en de geselecteerde oorspronkelijke GeoTIFF als schema-3-capture. De tijdelijke credentialhoudende `downloadProduct`-URL wordt strikt gevalideerd maar niet opgeslagen; bucket/key/tijd/path worden geheimvrij in GeoTIFF-receiptprovenance gebonden. 27 gerichte tests en een echte publieke live identity/profile zijn groen; niet gewired | nee; nog geen pixeldecode, CRS/gridvalidatie, provider-QC, rain-ratefeit of toelating | officiële GeoTIFF-georeferentie en waardesemantiek exact decoderen, legacy-LCC-aanname verwijderen/verifiëren, onafhankelijke events, harde child CPU/RSS/deadline, NUC-soak en snapshotcutover |
| `meteofrance_radar_fetch` | Météo-France | legacy decoder blijft actief; geïsoleerde DPRadar-kandidaat bewaart exact het officiële `METROPOLE/LAME_D_EAU`-catalogusantwoord plus het geselecteerde oorspronkelijke 500 m `IPRN20_C_LFPW` HDF5-bestand. Producttijd, bestandsnaam, beide checksums en Etalab-attributie zijn gebonden; OAuth-responses en credentials worden nooit opgeslagen; 25 gerichte tests groen; niet gewired | nee; exacte raw, 5-minutenaccumulatie, 0,01 mm, 500 m, 3472×3472 en QC-range zijn officieel bewezen, maar er wordt bewust geen pixel gelezen | officiële HDF5-veld-/CRS-/gain-offset-/missing-undetect-/QC-mapping, echte golden captures, begrensde hard-childdecoder, held-out events, NUC-soak en snapshotcutover |
| `enterprise_rainrate_fetch` | NOAA ERR | legacy decoder blijft actief; geïsoleerde kandidaat bewaart exact S3 `ListObjectsV2`-XML + één geselecteerde finale `GLB-5` NetCDF-4, verifieert daarin nul ontbrekende en vijf unieke upstream-inputidentiteiten en decodeert schema-3-gebonden float32-raster/DQF; echte publieke golden en adversariële tests groen; niet gewired | nee | harde child CPU/RSS/deadline, onafhankelijke held-out DQF/grid/productvalidatie, NUC-soak en snapshotcutover |
| `hsaf_h40b_fetch` | H SAF H40B | legacy decoder blijft actief; geïsoleerde kandidaat bewaart exact de officiële FTP-`NLST`-respons en geselecteerde originele `h40_yyyymmdd_hhMM_fdk.nc.gz` vóór selectie/inspectie. PUM 1.3-grid, instantane `rr` in mm/h, packing, geostationaire projectie en verplichte `qind`-metadata worden zonder pixelread gecontroleerd; proces- en threadlock voorkomen dubbele zware capture; niet gewired | nee; satelliet-NRT “first guess”, geen grondradarwaarheid; legacy `qind >= 15` en ontbrekende `qind → 50` zijn niet officieel onderbouwd | echte geautoriseerde golden, veilige transportkeuze/risicoacceptatie voor plain FTP, aparte hard-childdecoder, exacte rr/missing/nul/qind/Mode_off-semantiek, held-out radar/pluviometerreplays, NUC/HA/Z2M-soak en snapshotcutover |
| `goes_rrqpe_fetch` | NOAA GOES RRQPE | legacy pad blijft actief; geïsoleerde G18/G19-kandidaat bewaart per satelliet exact het vorige en huidige `ListObjectsV2`-XML en de geselecteerde oorspronkelijke ABI-L2-RRQPEF NetCDF-4 vóór bounded HDF5-structuurinspectie. RRQPE/DQF, 5424×5424 en geostationaire fixed-gridmetadata zijn gebonden zonder pixelread; thread-/proceslock, absolute deadline, crashresume en anonieme fixed-host S3 zijn getest. G18 `Full` en G19 `Provisional` blijven uitsluitend provenance, geen kwaliteitsscore; niet gewired | nee; geen pixeldecode, DQF-codecertificatie, gridtransform, ground validation of operationele resourceclaim | hard-child RRQPE/DQF-decoder, Mode-3/4/leapsecond-goldens, officiële DQF-semantiek, onafhankelijke radar/pluviometer-events, NUC/HA/Z2M-soak en snapshotcutover |
| `goes_glm_fetch` | NOAA GOES GLM | alleen resultaat | nee | listingbewijs plus alle verwerkte GLM-bestanden |
| `eumetsat_li_fetch` | EUMETSAT LI | alleen resultaat | nee | catalogus/productresponses; tokens nooit opslaan |
| `netatmo_fetch` | Netatmo | bestaande handler blijft result-only; geïsoleerde kandidaat bewaart exact de `getpublicdata`-JSON vóór inspectie met credentialvrije bounding-boxcontext, token uitsluitend in de vluchtige Authorization-header, immutable structure-verdicts en netwerkloze receipt/capture-retry; 49 capturetests. Een afzonderlijke canonical decoder (17 tests) bewaart station, module, upstreamtijd, numerieke waarde en QC exact, maar houdt unit en tijdondersteuning expliciet ongecertificeerd en opaque; de bounded factset commit atomisch met één raw-authority-read; niet gewired | nee; geen rain-rate-, interval-, nat-, droog-, dekking- of grondbevestigingsclaim | officiële veld-/module-/unit-/tijdsemantiek + API-/opslagrecht + echte geautoriseerde golden + sitecrosswalk + globale queryplanning/rate-control + held-out stationevents + snapshotcutover |
| `synoptic_fetch` | Synoptic | bestaande handler blijft result-only; geïsoleerde kandidaat bewaart exact de Time Series JSON vóór inspectie met credentialvrije querycontext, duurzame validation-verdicts, absolute deadline en netwerkloze complete retry; 104 capturetests. De canonical decoder (14 tests) bewaart ieder station/sensorset afzonderlijk, bindt intervalwaarden alleen tussen twee bewezen opeenvolgende observatietijden en houdt een eerste of request-window cumulatief punt expliciet anders; native QC/provisional blijft provenance, nooit automatische waarheid. De volledige bounded factset commit atomisch met één raw-authority-read; niet gewired | nee; decoder levert alleen candidate facts en geen nat-/droog-/admissionbeslissing | geautoriseerde echte golden/tokenrun + sitecrosswalk/representativiteit + globale queryplanning/rate-control + held-out events + snapshotcutover |
| `metar_fetch` | AviationWeather METAR | bestaande handler blijft result-only; geïsoleerde globale exacte `metars.cache.csv.gz`-capture heeft duurzame validation-verdicts, absolute deadline en complete retry; 31 capturetests plus echte AWC-golden. De canonical decoder (22 tests; 5.111 echte golden-rijen) houdt present weather categorisch, `VC` uitsluitend vicinity, ontbrekend/stale/sensor-off nooit droog en accumulatiegroepen uitsluitend kwantitatief wanneer periode en tijd bewijsbaar zijn; de bounded factset commit atomisch met één raw-authority-read; niet gewired | nee; geen stationselectie-, representativiteits-, nat-/droog- of grondbevestigingsclaim | historische vorige-reportbinding voor onzekere accumulaties + sitecrosswalk/QC/representativiteit + held-out events + snapshotcutover |
| `open_meteo_fetch` | Open-Meteo | legacy blijft result-only; de inert/ongewirede kandidaat bewaart de exacte publieke forecast-JSON vóór structurele inspectie en bindt een pseudonieme, desired-state-geautoriseerde querybatch, vaste tijdreekslengtes, cadence, units, contenthash en canonieke CC BY-attributionpolicy. Een afzonderlijke duurzame usage-authority bindt exacte `2*N`-requestcharge, vier UTC-vensters, one-shot permits, terms-latch, storecaps en een duurzame vijf-locatiegrens. De kandidaat zit nog niet in activation/child/recovery | nee; modelguidance is geen radar-/groundtruth en heeft nog geen held-out skillbewijs | vaste officiële modelbronauthority per regio sluiten; daarna activation/child/recovery/capacity/router/audit uitsluitend als één atomaire shadowset valideren |

Voor iedere rij gelden vóór omschakeling dezelfde gates:

1. bytes worden begrensd gelezen en vóór decode duurzaam opgeslagen;
2. iedere required member staat in één verifieerbare complete capture;
3. exact retry, crash tussen members, disk-full en corruptie zijn getest;
4. producttijd en receipttijd blijven afzonderlijk;
5. credentials, signed querystrings en tokens komen niet in provenance;
6. decoder- en productspecificatie zijn gecertificeerd;
7. alleen daarna kan de registryvlag voor operationele admission wijzigen.
