# Officiële bronbasis voor ground-semantiek

Status: shadow-only onderzoeksnotitie, geraadpleegd op 2 augustus 2026. Dit
document verleent geen operationele admission. Het legt alleen vast welke
betekenissen rechtstreeks uit primaire providerdocumentatie volgen en waar de
decoder dus juist géén extra betekenis mag verzinnen.

## Synoptic Time Series

Primaire bronnen:

- <https://docs.synopticdata.com/services/time-series>
- <https://docs.synopticdata.com/services/precipitation-service-explained>
- <https://docs.synopticdata.com/services/json-output-format>
- <https://docs.synopticdata.com/services/mesonet-data-qc>

Vastgelegde decoderregels:

- `precip=1` vervangt de ruwe neerslagvariabelen door provider-afgeleide
  `precip_intervals_set_1d` en `precip_accumulated_set_1d`.
- `precip_intervals` is neerslag tussen opeenvolgende observaties. Het is dus
  geen instantane intensiteit en heeft geen universeel vast uurinterval. Bij
  ASOS kan het interval tussen een officiële uurwaarneming en een special
  METAR liggen.
- Alleen volledig binnen het requestvenster liggende intervallen worden door
  de Time Series-service gerapporteerd. Een randinterval mag niet worden
  aangevuld of als nul worden aangenomen.
- `precip_accumulated` is de cumulatieve som binnen het aangevraagde
  tijdvenster. Het is geen onafhankelijk stationfeit over een vast
  meteorologisch interval en mag niet als `rain_live` worden gebruikt.
- `all_reports=1` kan meerdere onderliggende neerslagvariabelen/sensorsets
  opleveren. Sets blijven afzonderlijk; optellen of stil één set kiezen is
  verboden.
- Met `qc_remove_data=off` worden ook geflagde waarden teruggegeven. Een
  QC-vlag is volgens Synoptic niet automatisch bewijs dat een waarde fout is.
  De decoder bewaart daarom de oorspronkelijke flags; een latere QC-laag neemt
  pas een bruikbaarheidsbesluit.
- Synoptic noemt de Weather API-data voorlopig. Ze mag niet zonder
  provider-/netwerkvalidatie als absolute ground truth worden behandeld.
- Een ontbrekende variabele, response-code zonder stations of lege array is
  geen nulmeting en geen bewijs van droogte.

## AviationWeather METAR

Primaire bronnen:

- <https://aviationweather.gov/data/api/>
- <https://aviationweather.gov/help/data/>

Vastgelegde decoderregels:

- `metars.cache.csv.gz` wordt eenmaal per minuut vernieuwd. Die cachecadans is
  niet de observatietijd of het meetinterval van een station.
- De remarkgroep `Prrrr` geeft water-equivalent sinds de vorige METAR, in
  honderdsten inch. Ook al noemt AWC dit “hourly precipitation”, een special
  METAR kan het werkelijke interval verkorten. Zonder de vorige stationreport
  is een exacte intervalstart dus onbekend.
- De `Prrrr`-groep wordt volgens AWC weggelaten wanneer sinds de vorige METAR
  geen neerslag voorkwam. In een gedecodeerde cache blijft afwezigheid echter
  alleen een provider-recordfeit; zonder stationscapability, complete raw
  reportsemantiek en intervalstart mag ze geen universele droogclaim worden.
- `6RRRR` is afzonderlijk een drie- of zesuursaccumulatie afhankelijk van het
  reporttype; `6////` betekent dat de hoeveelheid niet bepaalbaar is.
- `7R24R24R24R24` is een afzonderlijke 24-uursaccumulatie. Geen van deze
  accumulaties is een instantane rain rate.
- Present-weathertokens blijven categorisch. `VC` beschrijft vicinity en mag
  nooit als neerslag op het station worden vertaald.

## Netatmo getpublicdata

Primaire ingang:

- <https://dev.netatmo.com/apidocumentation/weather>
- <https://helpcenter.netatmo.com/hc/en-us/articles/17057248785298-Smart-Rain-Gauge>
- <https://helpcenter.netatmo.com/hc/en-us/articles/360021158711-How-does-the-rainfall-measurement-work>

De officiële Help Center-documentatie bewijst wél de algemene
instrument-/dashboardsemantiek: de regenmeter gebruikt kantelbakjes; het
dashboard toont neerslag in `mm/h` over het laatste voortschrijdende uur en
een afzonderlijke dagsom van middernacht tot middernacht. Dat is geen bewijs
voor een instantane intensiteit en ook nog geen bewijs dat een bepaald
`getpublicdata`-veld precies met een van die dashboardwaarden overeenkomt.

De officiële Weather API-pagina is bereikbaar, maar de publiek leesbare
documentweergave die voor deze audit beschikbaar was bevatte geen verifieerbare
velddefinitie voor `rain`, `rain_live`, `rain_60min` of hun precieze
tijdondersteuning. Namen uit legacycode, Home Assistant of een voorbeeldpayload
zijn geen productspecificatie.

Daarom geldt voor de huidige kandidaat:

- exacte Netatmo responsebytes, station/module-ID's, timestamptokens en
  waarden mogen als opaque native feiten worden bewaard;
- een waarde mag alleen als voortschrijdend uur- of dagtotaal worden
  geïnterpreteerd wanneer de exacte API-veldnaam en tijdondersteuning nog met
  primaire providerinformatie aan de decoderversie worden gebonden; de
  algemene dashboarddocumentatie volstaat daarvoor niet;
- eenheid, interval, instantane betekenis, nul-/missingsemantiek en
  capability blijven `unknown` totdat een exacte officiële productspecificatie
  of providerbevestiging duurzaam aan de decoderversie is gebonden;
- er volgt nooit automatisch regen, droogte, dekking of ground confirmation
  uit een veldnaam of een lege/missende module.

## Algemene admissionregel

Een parser die technisch een getal kan lezen is nog geen meteorologisch
gecertificeerde decoder. Providerfacts blijven afzonderlijk en kandidaat-only
tot units, interval, klok, stationcapability, QC, representativiteit en een
held-out echte replay onafhankelijk zijn bewezen.
