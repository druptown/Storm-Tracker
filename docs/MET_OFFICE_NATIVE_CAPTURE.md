# Met Office UK-radar: exacte native capturekandidaat

Status: lokaal, worker-only, niet gewired, niet gedeployed en niet operationeel
toegelaten.

## Bronnen en product

De kandidaat gebruikt uitsluitend de publieke Met Office Open Data S3-bucket:

- registry en licentiecontext:
  https://registry.opendata.aws/met-office-uk-radar-observations/
- Met Office radar-factsheet:
  https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/research/library-and-archive/library/publications/factsheets/factsheet_15-weather-radar-2020_2023.pdf
- EUMETNET OPERA ODIM_H5 2.2-specificatie:
  https://www.eumetnet.eu/wp-content/uploads/2017/01/OPERA_hdf_description_2014.pdf

De registry beschrijft het product als een UK-composiet van geschatte
oppervlakteregensnelheid in HDF5, met een nominale cadence van vijftien minuten
en publicatie binnen ongeveer twintig minuten. Dat maakt het product relevant,
maar niet automatisch ground truth: radar meet de atmosfeer, waarna processing
een schatting van regen aan het oppervlak levert.

De exacte file declareert `ODIM_H5/V2_2` en quantity `RATE`. De bijbehorende
officiële ODIM 2.2-tabel definieert `RATE` als rain rate in `mm/h`. Dat bewijst
de native eenheid; het bewijst nog niet dat iedere pixel kwalitatief bruikbaar
is of hoe Met Office-specifieke correcties, afwezige radars en compositing de
onzekerheid beïnvloeden.

## Exacte captureset

`storm_tracker_worker/met_office_native.py` bewaart één schema-3-capture met
exact drie vereiste leden:

1. `listing_previous`: het exacte S3 `ListObjectsV2`-XML voor de vorige
   UTC-dag;
2. `listing_current`: het exacte XML voor de actuele UTC-dag;
3. `odim_hdf5`: het exact geselecteerde oorspronkelijke HDF5-object.

Beide listings zijn duurzaam opgeslagen vóór XML-inspectie. Het geselecteerde
HDF5-object is duurzaam opgeslagen vóór HDF5-inspectie. Selectie over twee
aangrenzende UTC-dagen voorkomt een kunstmatige datakloof rond middernacht.
De objectdownload is met de exacte ETag via `If-Match` aan de listingrevision
gebonden. Redirects, afwijkende host/path/query, credentials in de URL,
onbegrensde responses, listingtruncatie, vreemde schemas en conflicterende
objectidentiteiten falen gesloten.

Een volledige retry leest niets van het netwerk en retourneert hetzelfde
capture-resultaat. Een partiële retry hergebruikt en verifieert de reeds
duurzame leden en haalt alleen ontbrekende leden opnieuw op. Producttijd en
receipttijd blijven overal afzonderlijke feiten.

## Publiek live golden-profiel van 2026-08-02

De read-only goldeninspectie selecteerde:

`radar/2026/08/02/202608020515_ODIM_ng_radar_rainrate_composite_1km_UK.h5`

Vastgelegde identiteit:

- objectgrootte: 482345 bytes;
- ETag: `cab59e871637a772dc15f01f55009924`;
- SHA-256: `854a29f7b3dd340554edbfc81e89cecb754db9469aefb437785210acee8141d2`;
- ODIM: `ODIM_H5/V2_2`, H5rad 2.2;
- object/product/quantity: `COMP` / `COMP` / `RATE`;
- raster: 2175×1725, float32, 1000 m, vaste UK transverse-Mercatorprojectie;
- opslag: gzip niveau 6, chunks 75×1725, 29 toegewezen chunks;
- verwerking: RADARNET 2021.12.1, Z-R `a=200`, `b=1.6`;
- nominale producttijd: 05:15:00 UTC;
- inputvenster: 05:14:08 tot 05:15:09 UTC.

Dat inputeinde negen seconden na de nominale producttijd toont waarom de code
geen uniforme frame-observatietijd verzint. Nominale producttijd,
input-starttijd, input-eindtijd, ontvangsttijd en publicatietijd blijven
afzonderlijk.

De structurele inspecteur leest geen enkele rasterpixel. Het golden-profiel is
daarom bewijs van exact raw, selectie en native structuur, niet van correcte
regenwaarden.

## Harde grenzen en retrycontract

- maximaal 2 MiB per listing en 20 MiB per HDF5-object;
- streaming reads met begrensde chunks en een vaste timeout per request;
- vaste publieke S3-host, HTTPS, geen redirects en geen credentials;
- maximaal 1000 listingitems en geen getrunceerde listing;
- exacte object key, kwartierslot, ETag, lengte en S3-checksumprofiel;
- complete capture vereist alle drie rollen en cryptografische raw-authority;
- structurele drift levert `unknown`, nooit een meteorologische fallback;
- resultaat blijft `captured_not_decoded`, `weather_state=unknown` en
  `operational_admission_eligible=false`.

## Testbewijs

De gerichte suite bevat 31 tests voor onder meer daggrenzen, selectie,
schema- en opslagdrift, SSRF/redirects, streaming bytegrenzen, exacte
membersets, durable-before-parse, crashherstel, partiële en volledige retries,
corruptie, transporttimeouts, concurrency en bewijs dat service en Home Assistant de
kandidaat niet importeren.

## Nog gesloten releasegates

1. een afzonderlijke, hard begrensde schema-3-gebonden `RATE`-pixeldecoder;
2. provider-specifieke bevestiging van nodata/undetect, QC en
   compositingsemantiek; de `mm/h`-eenheid zelf volgt al uit ODIM 2.2;
3. onafhankelijke held-out UK-events en vergelijking met ground evidence;
4. uitvoering binnen de harde child CPU/RSS/deadlinegrens;
5. atomische koppeling aan globale raw-capacity en power-lossreconciliatie;
6. cadence-, latency-, disk-, queue- en thermische NUC-soak;
7. compacte worker-snapshotpublicatie en expliciete canary/cutover/rollback.

Geen van deze gates wordt door exact raw alleen overgeslagen.
