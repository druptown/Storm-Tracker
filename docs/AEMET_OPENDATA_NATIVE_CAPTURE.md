# AEMET OpenData exact-native capturecandidate

Status: **shadow-only, fail-closed en volledig onbedraad**. Deze module wijzigt
geen service, scheduler, providerregistry, Home Assistant-configuratie, versie
of deployartefact. Hij decodeert geen pixel en publiceert geen weerfeit.

## Afgebakende officiële bron

De kandidaat gebruikt uitsluitend het door AEMET gedocumenteerde endpoint:

```text
GET https://opendata.aemet.es/opendata/api/red/radar/nacional
api_key: <transportsecret in HTTP-header>
```

De officiële OpenAPI 3.0.1-specificatie definieert:

- de server `https://opendata.aemet.es/opendata`;
- operatie `GET /api/red/radar/nacional`;
- een periodiciteit van 30 minuten;
- een succesvolle JSON-response met de vier verplichte velden `descripcion`,
  `estado`, `datos` en `metadatos`;
- `api_key` als een `apiKey` security scheme met `in: header`.

Bron: <https://opendata.aemet.es/AEMET_OpenData_specification.json>.

De radarhelp van AEMET bevestigt alleen de volgende productclaims:

- nationaal composiet van de laagste PPI-reflectiviteit, behalve Canarias;
- meteorologische grootheid reflectiviteit in `dBZ`;
- GeoTIFF, gereprojecteerd naar `EPSG:4326`;
- de bestandsspecifieke `ESCALA` koppelt RGBA-kleuren aan intervallen van de
  meteorologische variabele.

Bron: <https://www.aemet.es/en/eltiempo/observacion/radar/ayuda>.

Deze claims zijn uitsluitend provenance. Ze certificeren geen regenintensiteit,
gridgeometrie, tijdsupport, QC of operationele toelating.

## Exacte schema-3-memberset

Eén voltooide capture bevat exact drie rollen:

1. `opendata_envelope_json`: alle oorspronkelijke HTTP-bodybytes van de
   succesvolle API-envelope;
2. `opendata_data_response`: alle oorspronkelijke HTTP-bodybytes die de
   gevalideerde `datos`-URL retourneert;
3. `opendata_metadata_response`: alle oorspronkelijke HTTP-bodybytes die de
   gevalideerde `metadatos`-URL retourneert.

De envelope wordt eerst als immutable schema-3 receipt opgeslagen. Pas daarna
wordt JSON geparsed en worden de twee selectielinks uitgelezen. Iedere gekozen
response wordt eveneens volledig begrensd en opgeslagen voordat enige volgende
productstap mogelijk is. De kandidaat inspecteert de data- en metadatabody niet.
Pas wanneer alle drie receipts bestaan, commit `RawArtifactStore` atomair de
complete capturemanifest.

Een afgebroken poging kan dus receipts achterlaten, maar nooit een vals
`complete` capturebewijs.

## Secret- en netwerkgrenzen

`STV3_AEMET_API_KEY` is alleen transportmateriaal:

- alleen de vaste API-request krijgt de header `api_key`;
- de sleutel staat nooit in de URL;
- jobpayloads moeten exact leeg zijn;
- de sleutel komt niet in raw bytes, receipts, provenance, upstream-ID's,
  capture-ID's, resultaatobjecten of foutteksten;
- responsebody of veilige responseheader die de sleutel toch echoot, wordt vóór
  raw-opslag verworpen;
- transportexceptions worden niet doorgeketend omdat een requestobject de
  geheime header kan bevatten.

Netwerkverkeer gebruikt geen omgevingsproxy, weigert redirects en aanvaardt
alleen HTTPS zonder credentials, expliciete poort, query of fragment. Het vaste
API-pad is exact gepind. `datos` en `metadatos` moeten verschillend zijn en
vallen onder:

```text
https://opendata.aemet.es/opendata/sh/<begrensd-token>
```

De tijdelijke downloadlinks worden buiten de exacte envelopebody niet
opgeslagen. Receipts en capture-identiteit binden er alleen een SHA-256 van.

## Resource-, deadline- en herstelcontract

Harde bovengrenzen:

| response | maximum |
|---|---:|
| envelope | 64 KiB |
| data | 96 MiB |
| metadata | 4 MiB |

Iedere leeslus is bounded, valideert een aanwezige `Content-Length` en weigert
transportcompressie. De volledige poging deelt één absolute monotone deadline
van maximaal 75 seconden. Een in-process threadslot én cross-process filelock
serialiseren de providercapture; beide respecteren dezelfde deadline.

Identiteiten zijn deterministisch uit provider, product, job-ID en rol. Bij
retry worden reeds duurzame members cryptografisch herladen en tegen de exacte
envelopeselectie geverifieerd. Alleen ontbrekende members worden opnieuw
opgehaald. Een complete capture kan volledig offline worden herverifieerd,
zonder sleutelresolutie of netwerk. Gewijzigde objectbytes, receipts,
selectiehashes of manifesten falen gesloten in `RawArtifactStore`.

## Bewust gesloten meteorologische gates

Het resultaat blijft altijd:

- `weather_state = unknown`;
- `capture_state = captured_not_decoded`;
- geen kwantitatieve rain-rate;
- geen cellen, tracking, forecast of operationele admission;
- `pixel_values_read = false`.

Open bewijs- en ontwikkelgaten:

1. een officiële actuele golden envelope/data/metadata-set met checksums;
2. exacte GeoTIFF-dimensies, affine transformatie, pixel-is-area/point en
   axisvolgorde per productrevisie;
3. veilige, bestandsspecifieke `ESCALA`-decoder;
4. exacte tijdzone en temporeel observatiesupport;
5. onderscheid NoData, geen echo en geen operationele radardekking;
6. pixel-QC en composietselectiesemantiek;
7. een meteorologisch gevalideerde, contextafhankelijke reflectiviteit-naar-
   regenrelatie; een vaste Z-R-formule wordt niet als waarheid aangenomen;
8. held-out replayvalidatie, capaciteit/thermiek en canarybewijs.

## Tests en optionele live probe

`tests/test_worker_aemet_native_capture.py` bewijst onder meer exacte rollen en
bytes, parsevolgorde, secretlekkage, URL/host/redirect/proxygrenzen,
responsegroottes, absolute deadline, thread- en cross-processlocking,
partial-resume, abrupte procescrashes, offline replay, raw-mutatie en afwezigheid
uit live wiring.

De live test is standaard overgeslagen. Hij draait alleen wanneer beide bewust
zijn gezet:

```text
STV3_RUN_AEMET_NATIVE_LIVE=1
STV3_AEMET_API_KEY=<secret>
```

Ook die test creëert slechts een lokale schema-3 shadowcapture en verricht geen
deploy of operationele admission.
