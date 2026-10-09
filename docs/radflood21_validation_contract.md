# RADFLOOD21 offline validatiecorpuscontract

Status: volledig lokaal, fail-closed en ongewired. Dit contract is geen live
provider, downloadt de 2,6 GB ZIP niet, extraheert niets en leest geen HDF5-
pixels. Het verandert geen registry, scheduler, admission, configuratie, versie
of deploybestand.

## Officiële corpusidentiteit

De gepinde autoriteit is Zenodo-record `7740059`, conceptrecord `7740058`:

- record-DOI: `10.5281/zenodo.7740059`;
- concept-DOI: `10.5281/zenodo.7740058`;
- recordrevisie tijdens de audit: `8`;
- publicatiedatum: `2023-03-16`;
- laatste gepinde recordupdate: `2023-10-17T12:06:57.625415+00:00`;
- toegang: `open`;
- licentie: `CC-BY-4.0`;
- makers: Edouard Goudenhoofdt, Michel Journée en Laurent Delobbe, alle drie
  verbonden aan het Koninklijk Meteorologisch Instituut van België.

De exacte officiële bestanden zijn:

| Rol | Bestandsnaam | Bytes | Officiële Zenodo-MD5 |
|---|---:|---:|---|
| Validatiecorpus | `radflood21.zip` | 2.642.551.002 | `2cb3660a9a2b81fcc3fb98d103984e12` |
| Alleen visualisatie | `radflood21.mp4` | 45.375.862 | `ab87ef2df8bf0008a39dd6b791168ea2` |

MD5 wordt uitsluitend gebruikt omdat Zenodo precies die upstream-checksum
publiceert. Bij lokale verificatie berekent de validator in dezelfde streaming
pass ook SHA-256, maar die lokale SHA-256 wordt niet voorgesteld als een door
Zenodo gepubliceerde autoriteit.

Het kleine lokale manifest staat in
`storm_tracker_worker/radflood21_official_manifest.json`. De semantische,
canonieke JSON-SHA-256 is
`f70b360db33e2d0aac5a052183d97a42a89ee3b9dc1a6916b9125ca53aefa741`.
Ontbrekende metadata, een andere recordrevisie, licentie, maker, bestandsset,
bytegrootte of checksum veroorzaakt een harde fout.

## Officieel bewezen meteorologische betekenis

RADFLOOD21 is een radar-gebaseerde kwantitatieve neerslagschatting die na
radar-QC met automatische regenmeters wordt samengevoegd. De KMI-gids en het
HESS-artikel bewijzen:

- projectie: Belgian Lambert 2008;
- ruimtelijke resolutie: 1 km;
- ruimtelijk support: iedere schatting is het gemiddelde over een vierkant van
  1 km bij 1 km, dus geen puntmeting;
- opgegeven dekking: 0,3° W–9,7° E en 47,4° N–53,7° N;
- tijdreferentie: UTC;
- gepubliceerde productperiode: 2021-07-11 00:00 UTC tot 2021-07-18 00:00 UTC;
- kernperiode van de driedaagse HESS-analyse: 2021-07-13 06:00 UTC tot
  2021-07-16 06:00 UTC;
- producten: accumulatie over 5 minuten om de 5 minuten en accumulatie over
  1 uur om het uur;
- de timestamp is het **einde** van de accumulatieperiode;
- de Zenodo-beschrijving noemt HDF5 voor de data;
- de productgids beschrijft GeoTIFF als `int16` met schaal 100
  (`1 = 0,01 mm`) en HDF5 als enkelvoudige-precisie floating point volgens
  ODIM.

De gids beschrijft ook de processing: QC van calibratiebias, statische en
residuele clutter, terreinblokkering en verticale profielen; compositing van
de drie dichtste radars binnen 180 km; optical flow voor 5-minutenaccumulaties;
en een ruimtelijke correctiefactor uit de combinatie van 1-uursaccumulatie en
regenmeters.

Deze informatie maakt het corpus waardevol voor latere offline validatie, maar
is nog geen exact HDF5-veldcontract.

## Waarom de decoder hard geblokkeerd blijft

De lokale manifest- en bestandvalidator bewijst uitsluitend corpusidentiteit.
`decoder_eligible`, `live_provider_eligible` en
`operational_admission_eligible` zijn altijd `false`. De open gates zijn:

- `zip_internal_member_inventory_not_pinned`;
- `hdf5_dataset_paths_not_pinned`;
- `odim_product_and_quantity_tokens_not_pinned`;
- `hdf5_gain_offset_nodata_undetect_not_pinned`;
- `pixel_qc_fields_and_codes_not_pinned`;
- `exact_grid_dimensions_origin_and_projdef_not_pinned`;
- `archive_member_filename_timestamp_contract_not_pinned`;
- `published_window_endpoint_inclusivity_not_pinned`;
- `official_small_golden_hdf5_not_available`.

De KMI-gids legt uit waar ODIM doorgaans `what`, `where` en `how` bewaart,
maar publiceert voor dit exacte archief niet de concrete datasetpaden,
`product`-/`quantity`-tokens, gain/offset, NoData, undetect, QC-velden,
griddimensies, oorsprong of PROJ4-string. Die mogen dus niet worden geraden.

## Offline gebruik

`storm_tracker_worker/radflood21_validation_contract.py` kan zonder netwerk:

1. het gepinde lokale manifest valideren;
2. een reeds verkregen Zenodo-JSON-snapshot vergelijken;
3. een reeds lokaal aanwezig bestand volledig streamen en grootte plus MD5
   controleren, zonder ZIP-extractie;
4. uit een expliciete UTC-eindtijd exact het 5-minuten- of uursupportinterval
   afleiden.

De officiële ZIP wordt niet als testfixture aangemaakt of gedownload. Tests
gebruiken kleine synthetische bestanden en gemuteerde metadata om succes,
ontbrekende velden, checksumafwijking, grootteafwijking en gesloten admission
te bewijzen.

## Uitsluitend primaire bronnen

- Zenodo-record:
  https://zenodo.org/records/7740059
- Officiële Zenodo-API-recordmetadata:
  https://zenodo.org/api/records/7740059
- KMI-casepagina:
  https://opendata.meteo.be/cases/202107.php
- KMI RADFLOOD21 user guide versie 2:
  https://opendata.meteo.be/cases/202107/radflood21_userguide.pdf
- HESS-artikel:
  https://hess.copernicus.org/articles/27/3169/2023/
- HESS-DOI: `10.5194/hess-27-3169-2023`

Auditdatum: 2 augustus 2026.
