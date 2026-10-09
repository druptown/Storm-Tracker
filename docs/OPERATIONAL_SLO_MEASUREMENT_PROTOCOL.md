# Storm Tracker V3 — operationeel SLO-meetprotocol

Status: preregistratie voor een latere kandidaatsoak; nog niet uitgevoerd

## Doel

Dit protocol levert de operationele `MetricEvidence` voor het vergrendelde
release-manifest. Het vergelijkt dezelfde Home Assistant-, MQTT- en
Zigbee2MQTT-belasting eerst met Storm Tracker volledig uitgeschakeld en daarna
met de worker-only kandidaat. Een gemiddelde mag nooit een slechte p95/p99 of
event-loopstall verbergen.

## Twee meetfasen

1. **Integration-off-basislijn**: Storm Tracker-integratie en worker uit; de
   overige HA-, recorder-, MQTT- en Z2M-configuratie blijft ongewijzigd.
2. **Kandidaat-shadow**: alleen de te beoordelen worker-only build wordt
   geactiveerd; dezelfde apparaten, recorderinstellingen, broker en normale
   huishoudelijke automatiseringen blijven actief.

Elke fase krijgt een eigen immutable `run_id`, UTC-start/einde,
hostboot-identiteit, HA-/OS-/Mosquitto-/Z2M-versie, integratie- en workerhash,
configuratiehash en ruwe meetartefacthash. Een fase met een configuratiewijziging
wordt niet met de andere fase gecombineerd.

## Meetautoriteit en klokken

- De NUC-klok, brokerklok en eventuele externe probe moeten vóór en tijdens de
  meting aantoonbaar gesynchroniseerd zijn.
- Iedere sample bevat zowel bron- als ontvangsttijd en een monotonisch
  volgnummer. Clockstep, tijdterugloop of te grote skew maakt het betrokken
  interval ongeldig en zichtbaar; het wordt niet stil verwijderd.
- De meetprobe publiceert geen meteorologische conclusie en mag geen
  providerfetch, databasequery of dashboardrender starten.
- Ruwe samples worden append-only bewaard; afgeleide percentielen verwijzen naar
  hun exacte bronartefact-SHA-256 en analysemethodeversie.

## HA-eventloop

- Een lichte periodieke callback meet `actual_monotonic - scheduled_monotonic`.
- De cadence en probe-implementatie zijn in beide fasen identiek.
- Gerapporteerd worden samplecount, ontbrekende volgnummers, p50/p95/p99,
  maximum en het aantal stalls strikt boven 100 ms.
- De kandidaat faalt bij p99 > 20 ms, kandidaat-minus-basislijn p99 > 10 ms of
  één aan Storm Tracker toewijsbare stall boven 100 ms.
- Naast de probe bewijst statische/runtime-telemetrie dat HA geen zware Storm
  Tracker-runtime aanroept: providers, decode, tracking, groundfusion,
  kalibratie, bulkserialisatie en meteorologische databasescans blijven exact
  nul.

## Zigbee2MQTT

- Een representatieve set Z2M-events krijgt een bronvolgnummer en
  broker-ingress-timestamp; de overeenkomstige HA-state-update krijgt hetzelfde
  correlatie-ID en ontvangsttijd.
- Duplicaten, gaten en omkering worden over de volledige sequence gecontroleerd.
- Gerapporteerd worden p50/p95/p99 en maximum van ingress→HA-state, afzonderlijk
  per device/cluster en globaal.
- De kandidaat faalt bij verlies, dubbele verwerking of volgordefout > 0, bij
  p95-regressie > 50 ms of p99-regressie > 100 ms.
- Een device zonder voldoende gecorreleerde events blijft `indeterminate`; het
  wordt niet uit de slechtste stratum verwijderd.

## Worker en MQTT

- Voor iedere geaccepteerde job worden enqueue-, lease-, start-, terminal- en
  publicatie/PUBACK-tijden exact gekoppeld.
- Jobs zonder terminal receipt, raw/observation/batchverlies, queue-overflow en
  publicatie zonder echte QoS 1 PUBACK moeten exact nul zijn.
- Queue-age wordt gedeeld door de actuele cadence van het bijbehorende product;
  p95 moet < 1,0 productcadans blijven.
- Een operationele snapshot moet uiterlijk na twee gemiste productcadansen stale
  zijn. Autoriteitsmismatch, verlopen snapshot die `current` blijft en
  retained-publicatie zonder actuele desired-state-SHA moeten exact nul zijn.
- Disconnect-, reconnect-, workerrestart- en hostrestartintervallen blijven in
  de meting en worden apart gemarkeerd; zij worden niet als outlier verwijderd.

## CPU, temperatuur en throttling

- Runtimebewijs legt de effectieve worker-affiniteit, numerieke threadlimieten,
  proces-nice en de gereserveerde fysieke core vast.
- Vereist zijn exact twee numerieke workerthreads, `nice 15` en minstens één
  volledige fysieke core buiten worker-affiniteit.
- Package temperature wordt als contexttijdreeks gerapporteerd. Een korte piek
  tot 100 °C is op zichzelf geen afkeurcriterium.
- Kernel-/hardwaremeldingen van thermalthrottling moeten exact nul zijn. Een
  temperatuurpiek samen met queue-, runtime-, HA- of Z2M-verslechtering wordt
  als afzonderlijk incident onderzocht en niet gladgemiddeld.

## Opslag

- Per store worden begin/eindgrootte, gebruikte SQLite-pagina's, freelist, WAL,
  raw-objecten, receipts, checkpoints, archiefsegmenten en vrije filesystembytes
  vastgelegd.
- Groei wordt conservatief uit het slechtste representatieve steady-state-
  interval geëxtrapoleerd. Bij nul of ongeldige observatieduur is de projectie
  `indeterminate`.
- De kandidaat moet bij gemeten groei minstens zeven dagen schrijfruimte over
  hebben en vóór de grens fail-closed waarschuwen; raw data wordt nooit door een
  temperatuur- of latentieregel verwijderd.

## Duur en volledigheid

- Zowel de integration-off-basislijn als de kandidaatfase duurt minimaal
  604.800 aaneengesloten seconden. Een korte gunstige basislijn mag de
  regressiemeting dus niet kunstmatig verbeteren.
- Clocksteps, tijdterugloop en overschrijding van de vooraf vastgelegde
  klokkenskew moeten over beide fasen samen exact nul zijn; anders faalt de
  operationele klokintegriteitsgate.
- Geplande foutinjecties worden vooraf gemarkeerd; onverklaarde gaten maken de
  betrokken gate `indeterminate` of `fail`, nooit automatisch `pass`.
- Percentielen gebruiken één vooraf vastgelegde deterministische definitie en
  worden uit alle geldige samples berekend. Geen post-hoc trimming of selectie
  op gunstige regio, provider, apparaat of tijdvak.
- Zowel globale resultaten als slechtste relevante strata worden gepubliceerd.

## Vereiste eindartefacten

- immutable basislijn- en kandidaatmanifesten;
- ruwe samplebestanden met SHA-256;
- completeness-/gaprapport;
- deterministische analyseconfiguratie en versie;
- per releasegate de exacte `MetricEvidence` en provenance-ID's;
- incidentlijst voor restarts, disconnects, throttling en stalls;
- diskprojectie;
- ondertekende/gelockte release-manifest-evaluatie;
- deploy- en rollbackhashes van exact dezelfde build.

De twee gesloten runartefacten worden met
`scripts/build_operational_slo_evidence.py` gevalideerd. Dat hulpmiddel weigert
onbekende of dubbele velden, niet-eindige waarden, ontbrekende samples,
fase-/methode-/omgevingdrift en bestaande output; run- en seriehashes worden uit
de inhoud zelf afgeleid en de compacte bewijsbundel wordt atomair gepubliceerd.
Voor Z2M moeten basislijn en kandidaat exact dezelfde vooraf vastgelegde
device-/clusterstrata bevatten; de releasegate gebruikt de slechtste p95- en
p99-regressie van de globale reeks en alle strata, zodat een traag apparaat niet
in een gunstig globaal percentiel verdwijnt. Iedere HA-, globale Z2M- en
Z2M-stratumreeks heeft bovendien minimaal 100 volledige gecorreleerde samples
nodig; minder steun blijft `indeterminate`.

Zonder deze bundel blijft de operationele vrijgave `indeterminate`, ook als de
unit- en replaytests groen zijn.
