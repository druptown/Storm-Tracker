# KMI RADQPE en publieke radarcomposiet: shadow-adapter

Status: lokaal geïmplementeerd op 30 juli en opnieuw geaudit op 2 augustus
2026; geen deploy, geen live Home Assistant-wijziging, geen bronwissel.

## Uitkomst

De bestaande `providers/kmi.py` kan vandaag niet verantwoord door “RADQPE”
worden vervangen. Er zijn nu wel twee strikt gescheiden routes:

1. `kmi_radqpe_native`: het door KMI gedocumenteerde kwantitatieve
   neerslagproduct. Formaat en meteorologische eigenschappen zijn bekend,
   maar in deze repository ontbreekt een geautoriseerde actuele
   API-/FTP-/SFTP-toegang en een expliciet gebruiksrecht voor deze toepassing.
2. `kmi_public_radar_composite`: de anoniem bereikbare KMI GeoServer-laag
   `radar:belgian_rainfall_composite`. De exacte actuele GeoTIFF is
   downloadbaar en verandert elke vijf minuten, maar de publieke metadata
   bewijst niet dat dit native RADQPE is en declareert geen betrouwbare
   neerslagquantity, unit of nodata-semantiek.

De nieuwe adapter haalt daarom alleen een complete shadow-evidenceset op. Hij
maakt geen `Observation`, geen intensityklasse, geen droge meting en geen
waarschuwing.

## Officieel gedocumenteerd RADQPE

De primaire productspecificatie is de officiële
[KMI RADQPE User Guide](https://radli.meteo.be/uploads/media/63d3e7525ab68/radqpe-userguide.pdf?v20241107-0827=).
De gids documenteert onder meer:

- een real-time kwantitatieve radargebaseerde neerslagschatting voor België
  en omgeving;
- verwerking van de volumetrische metingen van Avesnois, Helchteren, Jabbeke
  en Wideumont;
- clutterdetectie, beam-blockagecorrectie, verticale
  reflectiviteitsprofielen, neerslagtype-afhankelijke Z-R-conversie,
  interpolatie, compositing en rain-gauge mean-field-biascorrectie;
- Belgian Lambert 2008 als native projectie;
- een rasterresolutie van 1 km;
- een rain-ratecomposiet om de vijf minuten met maximaal ongeveer vijf
  minuten productvertraging;
- 5- en 10-minutenaccumulatie om de vijf minuten, 1 uur om de vijf minuten en
  24 uur om het uur;
- GeoTIFF en ODIM-HDF5;
- native GeoTIFF als EPSG:3812/int16 met schaalfactor 100;
- `startdate`, `starttime`, `enddate`, `endtime`, `product`, `quantity`,
  `gain`, `offset`, `nodata`, `undetect`, `xscale`, `yscale`, `xsize`,
  `ysize`, `UL_x`, `UL_y`, `projdef` en optionele `how`-metadata voor ODIM;
- native bestandsnamen zoals
  `YYYYMMDDHHMMSS.rad.best.comp.rate.qpe.hdf`;
- een onafhankelijk dagelijks verificatieproces tegen het klimatologische
  regenmeternetwerk;
- een best-effort servicelevel;
- een archief vanaf 2017 dat op aanvraag beschikbaar is.

Het officiële [KMI-dienstenoverzicht](https://www.meteo.be/nl/diensten)
vermeldt levering onder meer via API of FTP/SFTP. De
[KMI-datapagina](https://www.meteo.be/nl/diensten/data) vermeldt dat een deel
gratis via het Open Data-platform beschikbaar is en dat andere data
aangevraagd moet worden. Daar staat geen publiek native
RADQPE-downloadcontract dat Storm Tracker nu veilig kan configureren.

## Publieke KMI GeoServer-route

Tijdens de actuele brede regenzone op 30 juli 2026 is read-only vastgesteld:

- OWS-endpoint:
  `https://opendata.meteo.be/geoserver/ows`;
- WMS-laag: `radar:belgian_rainfall_composite`;
- WCS CoverageId: `radar__belgian_rainfall_composite`;
- vijfminutencadans;
- WGS84-bounds ongeveer
  `-0.925465,47.420867,9.658415,53.804418`;
- publiek WCS-raster van 892 × 538 float32-cellen in EPSG:4326;
- anonieme `GetCapabilities`, `DescribeCoverage` en `GetCoverage` werken via
  de globale `/geoserver/ows`-route;
- workspace-specifieke WCS-routes kunnen HTTP 403 geven en zijn daarom niet
  de contractendpoint van deze adapter.

De relevante, secretvrije requests zijn:

```text
SERVICE=WMS
VERSION=1.3.0
REQUEST=GetCapabilities
```

```text
SERVICE=WCS
VERSION=2.0.1
REQUEST=DescribeCoverage
COVERAGEID=radar__belgian_rainfall_composite
```

```text
SERVICE=WCS
VERSION=2.0.1
REQUEST=GetCoverage
COVERAGEID=radar__belgian_rainfall_composite
FORMAT=image/tiff
SUBSET=time("<expliciete UTC-tijd>")
```

### Waarom dit nog niet als rain rate geldt

De publieke WCS-beschrijving noemt het rasterveld `GRAY_INDEX` en declareert
als unit `W.m-2.Sr-1`. Dat is geen neerslagunit. De op 30 juli ontvangen TIFF:

- bevat wel een WGS84 GeoTIFF-transformatie;
- bevat geen productnaam of quantity;
- bevat geen neerslagunit;
- bevat geen nominale tijd;
- bevat geen expliciete GDAL-nodata-tag;
- gebruikt wel negatieve `-999`-waarden, terwijl DescribeCoverage `NaN` als
  nil value declareert;
- is een door GeoServer uitgegeven EPSG:4326-raster en niet het
  gedocumenteerde native Lambert-2008-grid.

De WMS-stijl gebruikt plausibele neerslaggrenzen, maar een kaartstijl is geen
authoritatief datacontract. We mogen daar dus niet stilzwijgend `mm/h`,
`nodata=-999` of “native RADQPE” uit afleiden.

## Exacte live evidence van de regenzone

De lokale eigen-gebruikcapture staat in:

` .analysis/kmi_public_radar_composite_20260730_capture_1 `

Hij bevat de exacte WMS-capabilities, WCS-beschrijving, fetch-receipt,
framevergelijking en drie eerste GeoTIFFs:

| Nominale UTC-tijd | Bytes | SHA-256 | Max ruwe waarde |
|---|---:|---|---:|
| 12:20 | 4.194.720 | `607b0d0d7be9eabea8ba29005122573311854ce4b9cca5dc7265f6275daa0ac8` | 56,85 |
| 12:25 | 4.194.720 | `cae3d7dd0ee6a2195b93ff90f536098f2c6703b5a003b10a81ba1f13c98af423` | 50,61 |
| 12:30 | 4.194.720 | `62336579d81d926e977929c7b50d60d04ad58eba98c0ffaf088c7abbdcd4c522` | 53,88 |

De drie hashes verschillen. Na het uitsluitend voor structurele vergelijking
uitsluiten van `-999` veranderden:

- 62.690 rasterwaarden tussen 12:20 en 12:25;
- 63.414 rasterwaarden tussen 12:25 en 12:30.

Dit bewijst dat de publieke route echte, temporeel veranderende rasterdata
levert. Het bewijst nog niet welke fysieke quantity of unit die ruwe waarden
hebben.

Omdat de regenzone nog actief was, is aansluitend ook een aaneengesloten
reeks vastgelegd in:

` .analysis/kmi_public_radar_composite_20260730_sequence_20 `

Deze tweede eigen-gebruikcapture bevat twintig vijfminutenframes van
11:15 tot en met 12:50 UTC, dus 95 minuten zonder tijdsgaten. Alle twintig
SHA-256-hashes zijn uniek. Per frame bevatten 52.851 tot 58.344 cellen een
positieve ruwe waarde. Tussen elk opeenvolgend frame veranderen, na het
uitsluitend voor deze structurele vergelijking uitsluiten van `-999`,
59.115 tot 64.401 rastercellen. De volledige lokale bundel omvat
84.060.312 bytes; de receipt heeft SHA-256
`ca9e89e32802c01cdae84ce8bff12b3b2fb0aaa9abd34fb2698a1da991f944dd`.

Ook deze reeks valideert transport, rastercoherentie, vijfminutencadans en
temporele verandering. Zij promoveert de bron niet tot kwantitatieve
neerslaginput zolang KMI de quantity, unit, nodata en productidentiteit niet
bevestigt.

## Licentie- en gebruiksgate

De WMS/WCS-capabilities vermelden `Fees none` en `AccessConstraints none`.
Dat bewijst dat de transportservice anoniem leesbaar is; het vervangt geen
productlicentie.

De officiële
[algemene voorwaarden voor data en diensten van het KMI](https://www.meteo.be/nl/diensten/algemene-voorwaarden)
beperken gegevens standaard tot eigen gebruik, tenzij een schriftelijke
overeenkomst anders bepaalt. Ze verbieden zonder zo'n overeenkomst onder
meer overdracht aan derden, reproductie/publicatie en diensten met
toegevoegde waarde.

Daarom staat zowel raw-retentie voor een productieroute als operationele
cutover op `BLOCKED` tot KMI schriftelijk bevestigt wat voor deze toepassing
toegelaten is. De lokale capture is een niet-gedistribueerde
ontwikkelingsaudit voor eigen gebruik.

Bij de audit van 2 augustus is geen native RADQPE-HDF/HDF5, geen native
EPSG:3812/int16-GeoTIFF en geen KMI API-/FTP-/SFTP-credential in de lokale
ontwikkelomgeving gevonden. De appv4-sleutel geeft uitsluitend toegang tot de
mobiele-appservice en is geen RADQPE-feedcredential. De publieke WCS-reeks kan
daarom niet als native golden fixture worden hernoemd.

## Geïmplementeerd contract

`engine/kmi_radqpe_contract.py` is byte-identiek aanwezig in Home Assistant en
worker. Het bevat:

- expliciete `kmi_radqpe_native`- en
  `kmi_public_radar_composite`-identiteiten;
- native productfacts uit de KMI-gids;
- WMS-capabilitiesparser met vaste bronidentiteit, WGS84-dekking,
  expliciete UTC-tijden en vijfminutencadans;
- WCS DescribeCoverage-parser die `GRAY_INDEX`, de foutieve unit en nil value
  zichtbaar bewaart;
- begrensde, enkelbands GeoTIFF-structuuraudit;
- exacte SHA-256-, vorm-, CRS-, transformatie-, bereik- en
  negatieve-waardestatistiek;
- een complete drie-artifactset:
  WMS-capabilities + WCS-beschrijving + één tijdspecifieke coverage;
- secretvrije, deterministische record identities;
- hard gecodeerde `detection_eligible=False`,
  `retention_eligible=False` en `operational_cutover=BLOCKED`.

`providers/kmi_public_radar_composite.py` is alleen een transportadapter.
De bestandsnaam noemt bewust geen RADQPE: de module haalt uitsluitend de
publieke composiet op en claimt nergens dat dit het native KMI-product of
`mm/h` is. `engine/kmi_radqpe_contract.py` blijft een overkoepelend
auditcontract, omdat het daarin juist noodzakelijk is om het officieel
gedocumenteerde native RADQPE-product en de niet-geïdentificeerde publieke
composiet als twee afzonderlijke identiteiten te modelleren:

- één officiële HTTPS-endpoint;
- identity encoding;
- expliciete media types;
- maximum 1 MiB voor catalogus, 1 MiB voor beschrijving en 16 MiB voor TIFF;
- maximaal 20 minuten oud en maximaal 60 seconden toekomstige clock skew;
- cross-check van WMS- en WCS-tijddomein/cadans;
- geen `plugin_id`, capability, priority, factory, `async_fetch` of
  `fetch_observations`;
- niet opgenomen in registry, policy, `__init__.py`, sensoren of dashboard.

## Status van RawArtifactStore en KMI-admission

Eén frame is geen zelfstandig volledig bewijsstuk: tijd, coverage en
semantische metadata zitten verdeeld over drie response bodies. Een veilige
retentieroute moet die set atomair en idempotent opslaan, zonder URL- of
credentialmateriaal en met een conflict bij hergebruik van dezelfde
nominale identity voor andere bytes.

De schema-3 worker-`RawArtifactStore` is inmiddels de enige beoogde raw
authority. De oudere schema-2-store is legacy en mag niet opnieuw als tweede
operationele waarheid worden gebruikt.

De huidige HA-side shadow-adapter retourneert drie exacte artifactbytes als
één **complete set in memory**, maar commit niets duurzaam. Om dat onderscheid
machineleesbaar te maken publiceert hij uitsluitend
`exact_raw_artifacts_complete_in_memory=true` en
`exact_raw_artifacts_durably_committed=false`; de eerdere dubbelzinnige vlag
`exact_raw_artifacts_complete` is verwijderd. Voor KMI ontbreken nog:

1. goedgekeurde gebruiks-/retentierechten;
2. bevestigde product- en quantitysemantiek;
3. een worker-only capture-adapter die een native bestand exact vóór decode
   als schema-3-receipt/capture commit; de publieke WCS-set blijft apart en
   semantisch geblokkeerd;
4. expliciete live shadow-configuratie, wiring en observability.

## Veilige cutovervolgorde

1. Vraag KMI schriftelijk of
   `radar:belgian_rainfall_composite` RADQPE rate is en laat quantity, unit,
   nodata, resampling en tijdsbetekenis documenteren.
2. Vraag de toegestane lokale retentie, replay, afgeleide verwerking,
   distributie en toegevoegde-waardetoepassing schriftelijk vast.
3. Geef de voorkeur aan native ODIM-HDF5-toegang wanneer KMI die levert,
   omdat quantity/gain/offset/nodata/undetect/projdef daarin
   artifactmetadata zijn.
4. Herhaal de nu uitgevoerde capture van twintig opeenvolgende echte frames
   voor andere relevante weertypes, plus ontbrekende en herstelovergangen.
5. Bewaar exact de upstream bytes en maak pas daarna een afzonderlijke
   analysis-grid.
6. Vergelijk de publieke/native route shadow-only met de bestaande
   appbeeldadapter, OPERA en onafhankelijke grondwaarnemingen.
7. Valideer detectie, cellifecycle, motion, passagekans en false alarms met
   event-held-out replays.
8. Voeg de bron pas daarna toe aan de operationele registry en policy.
9. Draai eerst een soak zonder waarschuwingseffect.
10. Verwijder of devalueer de oude KMI-appadapter pas nadat fallbackgedrag en
    rollback getest zijn.

## Wat de huidige regen nu oplevert

De brede regenzone is niet verloren: twintig opeenvolgende, exacte,
kwantitatief ogende rasters zijn met transportprovenance veiliggesteld en de
nieuwe shadowcode kan volgende actuele frames begrensd ophalen. De regen is
nergens afgekeurd. Alleen de meteorologische interpretatie wordt
tegengehouden zolang de bron die interpretatie zelf niet betrouwbaar
specificeert.
