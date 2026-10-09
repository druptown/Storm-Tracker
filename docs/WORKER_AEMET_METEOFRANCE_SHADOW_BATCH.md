# AEMET + Météo-France exact-raw shadowbatch

Status: lokaal actief in de gesloten worker-acquisitiematrix als
`active_shadow_raw`. Dit is uitsluitend een exacte raw-capturebevoegdheid. De
batch is niet gekoppeld aan `service.main`, Home Assistant, MQTT, een live
scheduler of meteorologische admission.

## Gesloten transportcontracten

| Workload | Rollen | Complete reservering | Deadline |
|---|---|---:|---:|
| `radar.aemet.composite` | OpenData-envelope (64 KiB), oorspronkelijke dataresponse (96 MiB), oorspronkelijke metadataresponse (4 MiB) | 101 MiB / 3 objecten | 75 s |
| `radar.meteofrance.dpradar` | DPRadar-catalogus (512 KiB), oorspronkelijk 500 m HDF5-bestand (40 MiB) | 48 MiB / 2 objecten | 90 s |

De childrequest bevat voor beide providers een exact leeg object. Credentials,
download-URL's, responsbytes, coördinaten en meteorologische feiten gaan niet
door de parent/child-controlgrens. AEMET vereist een geldige workerlokale
`STV3_AEMET_API_KEY`. Voor Météo-France voldoet óf een geldige statische
`STV3_METEOFRANCE_API_TOKEN`, óf de bestaande client-credentialroute via
`STV3_METEOFRANCE_APPLICATION_ID`. Alleen een constante presence-marker wordt
door de secretresolver teruggegeven; geen waarde wordt gehasht of opgenomen in
job, diagnose, receipt of capture.

## Bewijsgrenzen

- Beide native handlers bewaren iedere oorspronkelijke response vóór parsing
  of containerinspectie en hergebruiken een complete capture zonder netwerk of
  credentialresolutie.
- Childresultaten bestaan alleen uit begrensde technische aggregaten en
  hashes. `weather_state` blijft `unknown`, de capture blijft
  `captured_not_decoded` en `operational_admission_eligible` blijft `false`.
- Recovery bindt provider, product, rol, fetch-/capture-ID, contenttype,
  individuele en geaggregeerde bytegrens, exacte provenance, volledige
  ledenset, upstream capture-identiteit en producttijd. De crashruimte tussen
  duurzame raw commit en capaciteit-CAS is herstart-idempotent; provenance-,
  rol- en capture-identiteitstampering faalt gesloten.
- De storage-envelope reserveert één volledige tien-handlerbatch en gebruikt
  101 MiB als grootste actieve object/reserveringsgrens. De worker blijft
  geserialiseerd; dit voegt geen parallelle providerpolling toe.

## Resterende promotiegates

Raw-shadowactivering is geen productie-admission. Voor enige meteorologische
claim of live cutover blijven minimaal nodig:

1. expliciete, gedocumenteerde productie-retentie- en gebruiksrechten per
   provider;
2. echte actuele golden captures en held-out weersituaties, naast de huidige
   officiële voorbeelden en synthetische containerfixtures;
3. voor AEMET: gecertificeerde GeoTIFF-grid-, tijd-, `ESCALA`-, NoData/no-echo-,
   dekking- en pixel-QC-semantiek plus een verdedigbare reflectiviteit-naar-regen
   relatie;
4. voor Météo-France: gecertificeerde HDF5-veld-/quantity-, CRS/geotransform-,
   gain/offset-, missing/undetect- en QC-mapping;
5. begrensde hard-childdecoders, adversarial/held-out replays, NUC/NAS-soak en
   een afzonderlijk goedgekeurde snapshot/admission/cutovervalidatie.

