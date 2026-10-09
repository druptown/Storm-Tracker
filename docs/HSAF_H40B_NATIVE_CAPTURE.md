# H SAF H40B: exacte native capturekandidaat

Status: **actief in de gesloten lokale acquisition-shadowmatrix, capture-only,
niet gedecodeerd, niet door `service.main()` gewired en niet operationeel
toegelaten**

Native-contractaudit: **2 augustus 2026**; shadow-activatieaudit:
**9 augustus 2026**

## Besluit

De officiële productspecificatie is voldoende precies om het oorspronkelijke
H40B-bestand en het bewijs van de bestandsselectie exact, begrensd en
replaybaar op te slaan. Daarom bestaat nu een lokale schema-3-kandidaat in
`storm_tracker_worker/hsaf_h40b_native.py`.

De kandidaat bewaart exact, in deze volgorde:

1. de ongewijzigde FTP-`NLST`-datarespons waarmee het nieuwste officiële
   product wordt geselecteerd;
2. het ongewijzigde geselecteerde `h40_yyyymmdd_hhMM_fdk.nc.gz`-bestand;
3. pas daarna een atomisch capturemanifest dat beide immutable receipts en
   SHA-256-digests bindt.

De listing wordt vóór de selectie duurzaam gemaakt. Het gecomprimeerde
NetCDF-product wordt vóór gzip- of HDF5-inspectie duurzaam gemaakt. De
kandidaat leest geen `rr`-, `qind`-, `nx`- of `ny`-waarden en produceert geen
nat/droog-feit, radarcel, nowcast of forecast.

De module is gekoppeld aan de gesloten acquisition-childdispatcher en de
exacte crashrecoverybinding. Die kandidaatroute is alleen bereikbaar wanneer
de afzonderlijke `AcquisitionShadowRuntime` expliciet wordt samengesteld.
`service.main()` en de legacy productiejobtabel stellen die runtime niet samen
en importeren of dispatchen de native handler niet. De providerregistry bevat
alleen de bestaande auditreferentie; de legacy `hsaf_h40b_fetch` blijft
ongewijzigd. Er is dus geen live impact en geen deploymentwijziging.

De shadowregel is exact begrensd op twee rollen, 66 MiB totaal en 90 seconden.
Ze vereist de inseparabele worker-local bundel `STV3_HSAF_USERNAME` plus
`STV3_HSAF_PASSWORD`. De resolver geeft alleen een vaste aanwezigheidsmarker
door; gebruikersnaam, wachtwoord en marker komen niet in de jobpayload,
resultaat-evidence, receipts of diagnostiek terecht.

## Officieel productcontract

### Identiteit en meteorologische betekenis

De primaire autoriteit is EUMETSAT H SAF. Het product is:

- H SAF-product-ID `H40B`;
- acroniem `P-IN-FCI`;
- EUMETSAT-collectiereferentie `EO:EUM:DAT:1086`;
- “precipitation rate at ground by MTG FCI IR channel supported by LEO/MW”;
- een **instantane neerslagsnelheid**, niet een accumulatie over tien minuten;
- gebaseerd op MTG/FCI-infrarood, gecombineerd met beschikbare
  passieve-microgolfschattingen van LEO-satellieten;
- geproduceerd op het MTG FCI-grid, om de tien minuten, met ongeveer 2 km
  resolutie aan nadir;
- een full-disk invoergrid van 5568 × 5568 pixels.

De bestandsdatum `yyyymmdd_hhMM` is de nominale FCI-productsleuf in UTC. Ze
is geen begin- of eindtijd van een accumulatie-interval. De capture noemt dit
daarom uitsluitend `nominal_slot_time`.

De near-real-time run is volgens het PUM een **first guess**. LEO/MW-input kan
tot vier uur later aankomen; een verbeterde tweede run wordt vier uur later
gemaakt maar niet verspreid. H40B mag daarom niet als grondradarwaarheid of
pluviometerwaarheid worden behandeld.

Bronnen:

- [Product User Manual H40B/P-IN-FCI, SAF/HSAF/PUM-40B rev. 1.3](https://user.eumetsat.int/s3/ope-eup-strapi-media/SAF_HSAF_PUM_H40_B_1_3f_c2e477fe85.pdf)
- [EUMETSAT-catalogus EO:EUM:DAT:1086](https://user.eumetsat.int/catalogue/EO%3AEUM%3ADAT%3A1086)
- [Officiële H SAF-productkaart](https://hsaf.meteoam.it/service/product_map.php?data_group=precipitation)

### Native grid, CRS en velden

Het PUM 1.3 beschrijft een gzip-gecomprimeerd NetCDF4/HDF5-product dat
CF-1.7-conform is. De kandidaat accepteert alleen het volgende profiel:

| Onderdeel | Officieel contract |
|---|---|
| `nx` | float32, lengte 5568, `units=m`, `projection_x_coordinate` |
| `ny` | float32, lengte 5568, `units=m`, `projection_y_coordinate` |
| `rr` | int16, 5568 × 5568, instantane rain rate, `mm/h` |
| `rr.valid_min/max` | ruwe packed range 0…2000 |
| `rr.scale_factor` | 0,1 |
| `rr.add_offset` | 0,0 |
| `rr.missing_value` | -990 volgens de formele variabelentabel |
| `qind` | int8, 5568 × 5568, pixel quality index in procent |
| `qind.valid_min/max` | 0…100 |
| `qind.missing_value` | -99 |
| projectie | geostationair, sub-satellietlengte 0°, sweep-as `y` |
| perspective height | 35.785.831 m |
| ellipsoïde | semi-major 6.378.169 m; semi-minor 6.356.584 m |
| platform | `MTG1` |
| parallax | `Mode_on` of `Mode_off` |

Het PUM bevat in de begeleidende tekst een algemene zin over ontbrekende
waarden van -99, maar de formele variabelentabel specificeert voor `rr`
expliciet `missing_value=-990` en voor `qind` `-99`. De capture-inspector volgt
die typed variabelentabel en decodeert geen rasterwaarden.

`qind` combineert volgens het PUM de kwaliteit van de PMW-input met de
ouderdom/timeliness van de BT-versus-RR-relatie. De range 0–100 is bewezen,
maar het PUM definieert geen universele operationele nat/droog- of
acceptatiedrempel. `qind >= 15` is dus geen officieel H40B-criterium.

### Transport, beschikbaarheid en rechten

Voor de laatste zestig dagen publiceert het PUM de officiële FTP-route:

- host `ftphsaf.meteoam.it`, poort 21;
- map `/products/h40B/h40_cur_mon_data`;
- naam `h40_yyyymmdd_hhMM_fdk.nc.gz`;
- registratie is vereist voor gebruikersnaam en wachtwoord;
- oudere data moeten via het H SAF-archief worden aangevraagd;
- EUMETCast is een officieel alternatief distributiekanaal.

De gedocumenteerde FTP-route is klassieke, niet-versleutelde FTP. Daardoor
gaan credentials en data onversleuteld over het netwerk. De kandidaat houdt
de credentials uitsluitend gedurende één capture in geheugen, leest ze alleen
uit `STV3_HSAF_USERNAME` en `STV3_HSAF_PASSWORD`, en schrijft ze nooit naar
jobs, receipts, artifacts, resultaten of fouten. Dat voorkomt opslaglekken,
maar maakt de transportbeveiliging niet sterker. Een latere live toelating
vereist een expliciete risicoacceptatie of een officieel beveiligd alternatief.

De EUMETSAT-catalogus vermeldt “Free and unrestricted - CC-BY-4.0”. Het PUM
zegt daarnaast dat H SAF-producten kosteloos gebruikt mogen worden en dat per
gebruikt product het EUMETSAT-copyrightcredit zichtbaar moet zijn. Exacte
opslag en replay zijn daarmee toegestaan mits bron, licentie en vereiste
attributie behouden blijven.

Bronnen:

- [H SAF User Support en toegang](https://hsaf.meteoam.it/User/UserSupport)
- [H40B PUM 1.3, hoofdstuk Product availability](https://user.eumetsat.int/s3/ope-eup-strapi-media/SAF_HSAF_PUM_H40_B_1_3f_c2e477fe85.pdf)
- [EUMETSAT-aankondiging operationele MTG-producten](https://user.eumetsat.int/news-events/news/new-h-saf-mtg-products)

## Exacte capture-authority

De complete schema-3-capture heeft exact twee verplichte rollen:

| Rol | Exact artifact | Bindende identiteit |
|---|---|---|
| `listing_nlst` | ongewijzigde FTP-datarespons op `NLST /products/h40B/h40_cur_mon_data` | alleen ASCII-regels, alleen officiële map, exact officieel naamformaat, geldige UTC-datum op tienminutenrooster |
| `netcdf_gzip` | ongewijzigde FTP-datarespons van de geselecteerde `.nc.gz` | remote pad en basename gelijk aan listingselectie; optionele FTP-`SIZE` exact gelijk aan ontvangen bytes |

Selectie is deterministisch: het bestand met de laatste nominale slot-tijd
wordt gekozen. Dubbele basenames, dubbele nominale tijden, paden buiten de
officiële map, niet-ASCII-data, ongeldige kalenderdatums en slots buiten het
tienminutenrooster worden fail-closed afgewezen.

De listing en het product krijgen afzonderlijke receipts. Het complete
capturemanifest bindt hun content-SHA-256, receiptidentiteiten, geselecteerde
bestandsnaam en nominale producttijd. Een gedeeltelijke of gecrashte poging mag
losse immutable receipts achterlaten maar mag nooit als complete capture
worden gepresenteerd. Een retry hervat vanaf het aanwezige bewijs; een al
complete retry gebruikt netwerk noch credentials.

## Grenzen en structurele inspectie

Transport en inspectie zijn bewust begrensd:

- uitsluitend de vaste officiële FTP-host, poort, map en FTP-commando's;
- geen caller-geleverde host, map, bestandsnaam of credential in de jobpayload;
- FTP-I/O-timeout maximaal 25 s en totale capturedeadline maximaal 90 s;
- één procesbrede byte-lock plus één threadlock serialiseert alle H40B-
  captures binnen dezelfde raw store onder diezelfde absolute deadline;
- listing maximaal 2 MiB en 20.000 regels;
- gecomprimeerd product maximaal 64 MiB;
- gedecomprimeerd product maximaal 192 MiB;
- exact één complete gzip-member, inclusief geldige CRC/eindmarker;
- NetCDF4/HDF5-signature verplicht;
- maximaal 128 HDF5-links/objecten;
- alleen hard links; soft links en external links worden afgewezen;
- verplichte velden, shapes, dtypes, units, packing, projectie en globale
  identiteit moeten overeenkomen met PUM 1.3;
- `parallax_correction` wordt vastgelegd als `Mode_on` of `Mode_off`, maar niet
  operationeel geïnterpreteerd;
- geen enkele pixel- of coördinaatwaarde wordt gelezen.

De returnwaarde blijft daarom strikt:

- `capture_state=captured_not_decoded`;
- `weather_state=unknown`;
- `decode_status=not_decoded`;
- `forecast_eligible=false`;
- `operational_admission_eligible=false`;
- `admission_status=exact_raw_shadow_not_decoded_not_admitted`.

Een capturefout is uitsluitend “geen betrouwbaar bewijs verkregen”. Ze is
nooit bewijs voor droog weer, ontbrekende neerslag of een lege radarzone.

## Audit van de bestaande route

De bestaande implementatie in `storm_tracker_worker/satellite_radar.py` is
niet gewijzigd, maar heeft aantoonbare hiaten ten opzichte van dit contract:

1. ze gebruikt `/h40B/h40_cur_mon_data` in plaats van het in PUM 1.3
   gepubliceerde absolute pad `/products/h40B/h40_cur_mon_data`; dit kan alleen
   werken wanneer de FTP-login chrootgedrag toepast en is niet als contract
   bewezen;
2. ze bewaart de exacte NLST-respons en het originele `.nc.gz`-bestand niet;
3. ze decodeert direct en bewaart alleen afgeleide JSON, waardoor latere
   wetenschappelijke replay niet exact reproduceerbaar is;
4. ze vervangt een ontbrekend `qind`-veld door kunstmatige kwaliteit 50,
   terwijl PUM 1.3 `qind` als verplicht veld specificeert;
5. ze gebruikt `qind >= 15`, een drempel die niet uit het officiële PUM volgt;
6. ze behandelt de NRT first guess onvoldoende expliciet als
   satellietschatting met mogelijk verouderde PMW-kalibratie;
7. ze heeft geen expliciet operationeel beleid voor `Mode_off`
   parallaxcorrectie;
8. ze gebruikt plain FTP zonder dat transportbeveiligingsrisico in de output
   vast te leggen;
9. haar limiet van 8 MiB voor het gecomprimeerde product is niet door de
   officiële productspecificatie onderbouwd;
10. ze heeft geen atomische selection-plus-product capture en geen
    checksumgebonden evidenceketen.

Deze punten betekenen niet dat iedere oude H40B-waarde fout is. Ze betekenen
wel dat de oude route nog niet voldoende bewijsbaar, replaybaar of
meteorologisch toegelaten is.

## Tests en bewijsniveau

`tests/test_worker_hsaf_h40b_native_capture.py` controleert onder meer:

- overeenkomst van vaste productfeiten met de officiële contractfixture;
- exacte en secretvrije opslag van listing en product;
- opslag vóór listingselectie en vóór containerinspectie;
- deterministische selectie en UTC-tijdsemantiek;
- verplichte H40B-grid-, CRS-, unit-, packing- en QC-metadata;
- afwijzing van onveilig listingmateriaal, dubbelzinnige selectie,
  concatenated gzip, verkeerd formaat, ontbrekend `qind`, verkeerde unit en
  externe HDF5-links;
- crashpunten na elk receipt en vóór/na capturecommit;
- netwerk- en credentialvrije complete retry;
- afwijzing van credentials of transportparameters in jobpayloads;
- afwezigheid uit de productie-service en autonome scheduler, met uitsluitend
  een niet-dispatchende auditreferentie in de providerregistry;
- gesloten shadowbinding van workload, scope, twee rollen, bytegrens,
  deadline, credentialbundel en crashrecovery;
- blijvende `operational_admission_eligible=false` over childresultaat en
  herstelde evidence.

De contractfixture
`tests/fixtures/hsaf_h40b_official_contract.json` bevat alleen door officiële
bronnen bewezen productmetadata. De NetCDF-container in de tests is
synthetisch en bevat geen weerpixels. Dit is dus nog geen live golden-product
en geen bewijs dat een decoder meteorologisch correct is.

## Resterende harde gates

Voor enige decoder of live aansluiting moeten minimaal nog gebeuren:

1. één geautoriseerde echte schema-3-capture rechtstreeks van de officiële
   FTP-bron, zonder secrets in het bewijs;
2. vergelijking van het volledige echte containerprofiel met PUM 1.3 en
   vastlegging van eventuele productversievarianten;
3. afzonderlijke decoder die packed `rr`, `missing`, geldige nul en `qind`
   exact onderscheidt en nooit een ontbrekende QC verzint;
4. projectie- en coördinaatvalidatie tegen officiële geolocatiepunten;
5. expliciet, empirisch gevalideerd beleid voor `qind` en `Mode_off`, zonder
   arbitraire veiligheidsdrempel als meteorologische waarheid te verkopen;
6. checksumgebonden derived receipt en kwantitatieve evidence bridge;
7. held-out verificatie tegen pluviometers en onafhankelijke radarproducten in
   droge, stratiforme en convectieve situaties;
8. decoderisolatie in een begrensd hard child-process;
9. NUC-soak met bewezen nulimpact op Home Assistant en Zigbee2MQTT;
10. pas daarna shadowpublicatie, canary en een expliciete cutoverbeslissing.

Geen van deze gates wordt door de capturekandidaat overgeslagen.
