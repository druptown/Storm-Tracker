# Volledige HA-naar-worker workloadmanifest (shadow)

Status: lokaal, ongewired en niet gedeployed. De live integratie en worker
blijven uitgeschakeld.

`storm_tracker_worker/worker_cutover_manifest.py` maakt de totale ombouw
controleerbaar. Het manifest voert niets uit: het importeert geen provider,
plant geen taak en raakt Home Assistant niet. Het sluit wel iedere bekende
legacy-werklast in één content-addressed lijst op. Na de uiteindelijke cutover
mag uitsluitend dit in HA blijven:

- targets en relevante instellingen naar desired-state/CAS vertalen;
- één bounded workerbridge onderhouden;
- één immutable operationele snapshot O(1) projecteren;
- availability en `unknown/stale` tonen.

Alle providerdownloads, stationqueries, radar/satellietdecoders, gridwerk,
detectie, tracking, nowcast, grondfusie, kalibratie, verificatie, GeoJSON en
duurzame meteorologische opslag horen in de worker.

## Belangrijk nieuw gevonden gat

De eerdere provider-captureregistry was gesloten voor de bekende workerjobs,
maar niet voor alle netwerkactiviteit die nog in HA loopt. Drie
“validation-only”-providers worden vanuit `_poll_national_providers()` toch in
HA opgehaald:

- MeteoLux;
- GeoSphere Austria;
- ItaliaMeteo.

Ze produceren momenteel geen operationele radarobservaties, maar veroorzaken
wel HTTP-, JSON- en lifecyclewerk in HA. GeoSphere en MeteoLux zijn
location/model-validation; ItaliaMeteo combineert een historisch dagelijks
DPC-SRI-catalogusduplicaat en een optionele ICON-2I-modelquery via Open-Meteo.
Ze hebben geen workerjob en geen exact raw contract. Het manifest markeert dit
daarom expliciet als `blocked_gap`.

De regionale Blitzortung-pushsubscription is nu afzonderlijk gemanifesteerd
als `lightning.blitzortung.event_ingress`. Zij staat niet in de periodieke
acquisitiecatalogus, heeft geen cadence en mag bij stilte nooit een
absenceclaim opleveren.

## Geen valse “klaar”-claim

`foundation_only` betekent dat er een workerhandler of bouwsteen bestaat, niet
dat de meteorologische keten betrouwbaar is. `shadow_candidate` betekent dat
extra, ongewirede exact-native/decoder/evidencebouwstenen bestaan.
`blocked_gap` betekent dat zelfs het worker-equivalent nog ontbreekt.

De tests controleren onder andere dat:

- iedere provider uit de raw-captureregistry in het cutovermanifest staat;
- iedere meteorologische productiejob een eigenaar heeft;
- alle vier HA-intervalcallbacks expliciet zijn verantwoord;
- MeteoLux, GeoSphere en de twee ItaliaMeteo-rollen zichtbaar geblokkeerd blijven;
- de Blitzortung-eventingress exact één eigenaar heeft en cadence-disjunct is;
- slechts één providerloze HA-control-plane workload na cutover is toegestaan;
- het manifest zelf geen runtime-, netwerk-, storage- of HA-import heeft.

Dit manifest is de checklist voor de latere cutoverwiring en voorkomt dat een
“lichte” validatiebron ongemerkt opnieuw HA of Z2M vertraagt.

## Gesloten acquisitiedekking (manifest v4)

Iedere regel uit de worker-acquisitiecatalogus heeft nu exact één expliciete
eigenaar via `acquisition_workload_ids`. De test vergelijkt de twee catalogi
rechtstreeks: een nieuw providerproduct kan dus niet meer worden toegevoegd
zonder ook in het cutovercontract te verschijnen, en een workload kan niet
stil door twee runtimepaden worden geclaimd.

Daarnaast worden alle 21 externe Python-providermodules uit de legacy
Home-Assistant-integratie exact één keer geclassificeerd via
`legacy_provider_modules`. Interne helpers hebben een gesloten allowlist. Een
nieuw netwerkpad in HA kan daardoor niet als onschuldige helper buiten het
cutovermanifest vallen.

De gesloten cadencecatalogus bevat momenteel 27 workloads plus één apart
event-ingresscontract. KMI/KNMI, nationale en regionale radar,
satellietneerslag, METAR/Netatmo/Synoptic, Open-Meteo en
GOES/EUMETSAT-lightning zijn volledig benoemd. KMI is daarbij drie producten,
GeoSphere/MeteoLux zijn location validation, ItaliaMeteo is een
historical/modelsplit en GOES GLM blijft geblokkeerd op dynamic-member
recovery. “Opgenomen” betekent alleen dat een bron niet vergeten kan worden.
Het verleent geen meteorologische of operationele admission; ontbrekende
rechten, native semantiek, decoders, replays of resourcebewijzen houden hun
workload zichtbaar dicht.
