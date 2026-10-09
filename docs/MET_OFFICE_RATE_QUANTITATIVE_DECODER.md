# Met Office UK RATE-decoder (afgeschermde kandidaat)

Status: volledig **unwired**, niet-operationeel en niet toegelaten als weerbron.
De module `storm_tracker_worker/met_office_rate_quantitative.py` leest uitsluitend
een reeds complete schema-3-capture van `met_office_native.py`. Ze is niet
geregistreerd als workerhandler, provider, schedulerjob of Home Assistant-pad.

## Officiële semantische bronnen

- Met Office, *UK 1km Rainfall Radar Composite Data*: het product is een Britse
  composiet op 1 km², afgeleid van radarreflectiviteit tot een schatting van de
  oppervlakteregensnelheid, geleverd als HDF5, vier keer per uur:
  <https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/business/data/external-data/asdi-radar-1km.pdf>
- EUMETNET/OPERA, *ODIM_H5 2.2 specification*: `RATE` betekent rain rate in
  mm/h; `y = gain * x + offset`; `nodata` is een niet-beschikbare/lege ruwe
  code en `undetect` is de ruwe code onder de detectiegrens. De nominale
  root-tijd staat los van `dataset/what` start- en eindtijden:
  <https://eumetnet.eu/wp-content/uploads/2017/01/OPERA_hdf_description_2014.pdf>
- EUMETNET ODIM-overzicht en versielinks:
  <https://www.eumetnet.eu/support-to-members/aviation-policy/>

Deze bronnen rechtvaardigen alleen de productbetekenis en ODIM-regels. Ze zijn
geen bewijs dat dit specifieke bestand meteorologisch correct is en verlenen
geen operationele toelating.

## Exacte inputautoriteit

Eén decodepoging accepteert precies drie leden uit één complete
`met_office_uk_radar_selection_capture`:

1. `listing_previous`: de exacte S3 `ListObjectsV2`-XML voor de vorige UTC-dag;
2. `listing_current`: de exacte S3 `ListObjectsV2`-XML voor de huidige UTC-dag;
3. `odim_hdf5`: het exact geselecteerde HDF5-object.

De decoder controleert opnieuw:

- raw-store schema 3, capture- en receipt-schema, `complete=true`;
- provider, product, capture-fetch-ID, drie unieke rollen en alle memberbindings;
- receipt-ID, receipt-hash, content-hash, grootte en ontvangsttijd per lid;
- gesloten provenance-objecten, collector en collectorversie, bucket, operatie,
  HTTP-revisie, ETag, Last-Modified, contenttype en bytegrenzen;
- twee opeenvolgende UTC-dagprefixen, niet-afgeknotte gesloten XML-schema's,
  kwartier-slots, selectie van het nieuwste object en revisie-eenduidigheid;
- de exacte samengestelde `upstream_capture_id` uit key, ETag en de drie
  content-hashes.

`MetOfficeRateRawAuthority` haalt de capturebundel één keer uit de raw store.
Dezelfde in-memory, byte-geverifieerde bundel levert daarna de capture-receipt
aan de CanonicalEvidenceStore. Daardoor leest of hasht de evidencecommit de drie
raw bestanden niet opnieuw. Eén authority-instantie kan nooit van capture
wisselen.

## Vast ODIM-profiel

De decoder herhaalt het volledige profiel uit `met_office_native.py` voordat
een pixel semantische betekenis krijgt:

- `Conventions=ODIM_H5/V2_2`, `H5rad 2.2`, object/product `COMP`;
- exact één `dataset1/data1/data`, quantity `RATE`, geen onbekende groepen of
  attributen en geen qualitydataset;
- uitsluitend hard links; geen soft link, external dataset of virtual dataset;
- native float32, 2175 rijen × 1725 kolommen, 75 × 1725 chunks, gzip niveau 6
  en alle 29 chunks aanwezig;
- Met Office-transverse-Mercatorprojectie, 1000 m pixels en exact de vastgelegde
  hoekcoördinaten;
- `gain=1`, `offset=0`, `nodata=-1`, `undetect=0`;
- nominale bestands-/ODIM-tijd gelijk aan capturetijd; start/einde blijven twee
  afzonderlijke tijden binnen het toegestane kwartierprofiel;
- bron-WMO's, nodes, software-, taak-, Z/R- en processingflags volgens het
  geobserveerde gesloten profiel.

Het HDF-object wordt chunkgewijs gelezen met een deadlinecheckpoint voor en na
iedere chunk. Er vindt geen interpolatie, reprojicering, drempeltoepassing,
wet/droogclassificatie, celvorming of fallback plaats.

## Kwantitatieve omzetting

Voor elk gecodeerd float32-pixel `x`:

```text
nodata_mask   = (x == -1)
undetect_mask = (x == 0)
valid_mask    = not nodata_mask and not undetect_mask
RATE_mm_h     = 1 * x + 0, alleen waar valid_mask waar is
```

Gemaskerde waarden krijgen in `rate_mm_h` nul als neutrale opslagwaarde, maar
blijven ondubbelzinnig onderscheiden door de twee afzonderlijke maskers.
`rate_encoded` bewaart de oorspronkelijke float32-code. NaN, ±Inf en negatieve
waarden die niet exact `nodata=-1` zijn, leiden fail-closed tot een
`decode_error`; ze worden nooit geklemd of geïnterpreteerd.

Omdat de live capture geen ODIM-qualitydataset bevat, schrijft het artefact
expliciet:

```json
{
  "quality_dataset_present": false,
  "reason_code": "odim_quality_dataset_absent",
  "state": "unavailable"
}
```

Afwezigheid van QC wordt dus nooit als goede kwaliteit behandeld.

## Niet-operationeel artefact en bewijsreceipt

Het resultaat is een deterministisch, content-addressed `.morq`-object met:

- canonieke JSON-header en gesloten schema;
- afzonderlijk zlib-9 gecomprimeerde arrays;
- SHA-256 over elke gecomprimeerde en ongecomprimeerde array;
- capture-, drieleden-, bron-, tijd-, raster-, processing- en decoderversiebinding;
- expliciete `operational_admission_eligible=false`;
- maximale objectgrootte 64 MiB.

Publicatie gebruikt een per-store threadlock plus OS-processlock, fsync van
bestand en mappen, een stagingbestand en een content-addressed hard-link. Een
retry na een crash vóór of na de link hergebruikt/verifieert het object en ruimt
dezelfde stagingkopie op. Een bestaande hash met afwijkende grootte of inhoud
is corruptie, geen overschrijfbaar object. De store heeft onveranderlijke
profielgrenzen voor bytes, objecten en stagingobjecten.

Na herlezing en volledige artefactvalidatie commit de decoder een
CanonicalEvidenceStore-receipt. Die receipt bevat alleen hashes en metadata,
nooit raw bytes of een raw pad. Herhaling met dezelfde job/attempt en dezelfde
feiten is idempotent; hergebruik van dezelfde identiteit voor andere feiten is
een conflict.

## Geteste foutvlakken

De tests dekken onder meer:

- volledig succes, drie leden, maskers, gain/offset en de drie afzonderlijke
  tijden;
- één raw-authoritylezing over decode plus evidencecommit en idempotente retry;
- provenance-, XML-, key-, tijd-, raster-, quantity-, dtype- en ODIM-drift;
- NaN, Inf, onverwachte negatieve waarden, afkapping, soft/external links en een
  onverwachte qualitygroep;
- artefacttampering, object- en stagingcapaciteit, ENOSPC en SQLite-full;
- threadconcurrentie, OS-processlockpad en harde procescrash na publicatielink;
- absolute monotone deadlinecheckpoints;
- golden metadata van de read-only live capture zonder HDF5-bestand in Git.

De optionele test `STV3_MET_OFFICE_GOLDEN_HDF5=<absoluut pad>` kan lokaal de
vastgelegde SHA-256 controleren en alleen-lezen native rasterstatistieken maken.
Het live HDF5-object wordt bewust niet aan de repository toegevoegd.

## Resterende toelatingspoorten

Deze kandidaat mag pas worden gekoppeld nadat alle volgende poorten afzonderlijk
bewijs hebben:

1. **Officieel profielbewijs:** bevestiging dat het gesloten, geobserveerde
   Met Office-opslagprofiel stabiel/contractueel is of een expliciet beheerde
   profielversie met schema-driftprocedure.
2. **QC:** een betrouwbare qualitybron of wetenschappelijk gevalideerde aanpak
   voor het ontbreken ervan; `unavailable` is geen operationele kwaliteitsclaim.
3. **Held-out meteorologie:** vooraf vastgelegde, niet voor tuning gebruikte
   regenmeters/events voor detectie, intensiteit, ruimtelijke fouten, latency,
   missers en false positives in verschillende seizoenen en regimetypes.
4. **Hard child isolation:** de huidige deadline is coöperatief. HDF5-decode moet
   vóór live gebruik in de algemene hard-decoder-child met OS-memory-, CPU- en
   wallclocklimieten draaien en veilig op kill/timeout herstellen.
5. **NUC-budget:** benchmark met echte drukte naast Home Assistant en Z2M;
   aantonen dat CPU, RSS, I/O-wachttijd en temperatuur binnen het afgesproken
   budget blijven zonder HA/Z2M-latency.
6. **Operationele admission:** expliciete provider-adapter, versies, rollback,
   shadowrun en fail-closed admissionreview. Dit document of een
   `decoded_complete`-receipt is daarvoor nooit voldoende.

Tot die zes poorten aantoonbaar groen zijn, blijft de decoder een lokale,
afgeschermde onderzoeks- en replaykandidaat.
