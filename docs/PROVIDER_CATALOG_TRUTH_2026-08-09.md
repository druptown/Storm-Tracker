# Providercataloguswaarheid en onuitvoerbare limieten

Status: **lokaal contract, fail-closed, niet gedeployed**.

Deze wijziging corrigeert uitsluitend de gesloten planningcatalogus en het
HA-naar-worker-cutovermanifest. Zij activeert geen provider en wijzigt geen
handler, child, recoverypad, service-registratie, Home Assistant-code of live
scheduler.

## Omvang

De cadencecatalogus bevat nu exact **27** workload-identiteiten. Daarnaast
bestaat exact **één afzonderlijke event-ingress-identiteit**. De twee sets zijn
disjunct en hebben elk exact één eigenaar in het cutovermanifest.

## KMI: drie producten, drie identiteiten

| Workload | Product | Rol | Status | Productieretentie |
|---|---|---|---|---|
| `radar.kmi.appv4_visual` | gerenderde appv4-animatieafbeelding | categorische visualisatie | `decommissioned` | nee |
| `radar.kmi.public_wcs_unknown_quantity` | publieke `belgian_rainfall_composite` WCS-GeoTIFF | validatiebewijs met onbekende quantity | `unsupported_fail_closed` | nee |
| `radar.kmi.radqpe_native` | gedocumenteerde native RADQPE HDF5 | kwantitatieve radarkandidaat | `external_contract_blocked` | nee |

De appv4-afbeelding is geen native RADQPE en haar niet-gekalibreerde
paletpixels krijgen geen meteorologische admission. De publieke WCS verklaart
geen betrouwbare precipitation quantity, unit of nodata-semantiek en de
vastgepinde KMI-gate verbiedt productie-/value-added-retentie. Haar bekende
transiente audit-envelop is 1 MiB capabilities + 1 MiB description + 16 MiB
coverage = 18 MiB en drie rollen.

Voor native RADQPE bewijst de user guide product-, grid- en cadansfeiten, maar
er is geen bruikbaar extern deliverycontract, responseplafond, deadline of
credentialauthority. Daarom is het budget bewust `null`; de catalogus verzint
geen limiet. Deze workload levert geen intent en kan niet in dispatch komen.

De oude identiteit `radar.kmi.radqpe` is bewust geen transparante alias. Zij
was semantisch ambigu en migreert alleen via
`ambiguous_three_product_split_rebuild_required` naar de drie identiteiten
hierboven.

## Locatievalidatie en het ItaliaMeteo-duplicaat

`GeoSphereAustriaProvider` vraagt de INCA **punt-nowcast** op en
`MeteoLuxProvider` vraagt lokale weather/nowcastinformatie op. Zij zijn daarom
geen radarworkloads meer:

- `validation.geosphere.location` — model/location validation;
- `validation.meteolux.location` — model/location validation.

De voormalige ItaliaMeteo-radarclaim combineerde twee verschillende paden en
is gesplitst:

- `validation.italiameteo.dpc_sri_historical_catalog` is uitsluitend de
  dagelijkse/historische catalogus van het DPC-SRI-product; hij is geen
  realtime radar en dupliceert de directe DPC-bron;
- `model.openmeteo.italiameteo_icon2i.region` is de optionele ItaliaMeteo
  ARPAE ICON-2I modelquery via Open-Meteo; hij blijft geblokkeerd totdat één
  Open-Meteo-authority het modelpad bezit.

Alle vier workloads zijn location/model/historical validation, niet
operationele radar, en blijven zonder intent.

## Blitzortung is event ingress, geen periodieke acquisitie

`lightning.blitzortung.event_ingress` staat uitsluitend in
`event_ingresses`, met:

- deliverymodel `mqtt_push_event_stream`;
- status `ownership_blocked`;
- `cadence_seconds: null`;
- absence-semantiek `no_absence_claim`;
- geen admission en geen productieretentie.

Een stil interval, een QoS0-gap of een ontbrekend pakket bewijst dus nooit dat
er geen bliksem was. Workerownership blijft geblokkeerd totdat backpressure,
gapreceipts, durable raw decoding en soak bewezen zijn. De legacy-id
`lightning.blitzortung` migreert alleen naar event-ingressownership en kan geen
cadencejob worden.

## Gecorrigeerde harde enveloppen

| Workload | Transport | Rollen | Wall | Reden |
|---|---:|---:|---:|---|
| `radar.aemet.composite` | 101 MiB | 3 | 75 s | 64 KiB envelope + 96 MiB data + 4 MiB metadata, naar boven afgerond |
| `satellite.hsaf.h40b` | 66 MiB | 2 | 90 s | 2 MiB NLST + 64 MiB `.nc.gz`; gelijk aan native absolute deadline |
| `lightning.goes18.glm` | 68 MiB | 14 | 105 s | twee listings van 4 MiB + maximaal twaalf leden van 5 MiB |
| `lightning.goes19.glm` | 68 MiB | 14 | 105 s | idem |

GOES GLM blijft `unsupported_fail_closed` tot de volledige dynamische
memberselectie, exact-raw retentie en crashrecovery zijn bewezen. De hogere
envelop is dus geen activation. H SAF past aantoonbaar binnen 90 seconden en
blijft onder de 105-secondenlimiet; anders zou ook die workload fail-closed
moeten worden.

## Gesloten migraties

De catalogus publiceert vijf auditmigraties en
`transparent_aliases: false`:

| Legacy-id | Replacement(s) | Migratie |
|---|---|---|
| `radar.kmi.radqpe` | drie KMI-producten | ambigue semantische split; plan rebuild verplicht |
| `radar.geosphere.at` | `validation.geosphere.location` | semantische rename; rebuild verplicht |
| `radar.meteolux.lu` | `validation.meteolux.location` | semantische rename; rebuild verplicht |
| `radar.italiameteo.it` | historische catalogus + modelpad | split; rebuild verplicht |
| `lightning.blitzortung` | event-ingress-id | ownershipmigratie; geen cadence-alias |

Een oud plan kan dus nooit stil een nieuw product gaan betekenen.

## Nog geblokkeerd

- native KMI RADQPE: externe delivery-/credentialauthority en uitvoerbare
  limieten ontbreken;
- publieke KMI WCS: quantity/unit/nodata en retentierecht ontbreken;
- KMI appv4: visual only, gedecommissioneerd voor meteorologische input;
- GeoSphere/MeteoLux: exact-native workercontract ontbreekt;
- ItaliaMeteo: historische duplicatie en modelauthority nog niet opgelost;
- GOES-18/19 GLM: dynamische memberrecovery en exact-raw setretentie ontbreken;
- Blitzortung: worker-owned event-ingress/backpressure/gapbewijs/soak ontbreken.

Geen van deze blockers mag als `geen regen`, `geen bliksem`, providergezondheid
of een operationele admissionclaim worden geïnterpreteerd.
