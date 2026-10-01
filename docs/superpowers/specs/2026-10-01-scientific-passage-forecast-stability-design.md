# Wetenschappelijk stabiele buipassagevoorspelling

**Datum:** 1 oktober 2026

**Status:** wetenschappelijk herzien ontwerp; nog niet geïmplementeerd en nog
niet meteorologisch gevalideerd

**Bouwt voort op:**
`2026-09-23-footprint-ground-validation-replay-design.md`,
`VNEXT_NOWCAST_BASELINES_AND_FORECAST_CONTRACT.md` en
`VNEXT_FORECAST_VERIFICATION.md`

## Doel en wetenschappelijke claim

Storm Tracker moet voor iedere doelpositie één of meer naderende
neerslagpassages volgen en aankomst, vertrek en lokale radarintensiteit zo
vroeg mogelijk ramen. De operationele basis is een deterministische
radar-nowcast: een recent, volledig en kwantitatief neerslagveld wordt met een
toegelaten ruimtelijk bewegingsveld geadveceerd. Cel- en systeemobjecten dienen
voor identiteit, lineage en presentatie; zij zijn niet op zichzelf de lokale
regenvoorspelling.

De eerste implementatie is een Lagrangian-persistencebaseline. Zij neemt aan
dat de recente neerslagstructuur en intensiteit tijdens de korte nowcasthorizon
worden verplaatst. Zij claimt nog geen betrouwbare voorspelling van initiatie,
groei, verval, rotatie of sterke vervorming. Zulke evolutie mag later uitsluitend
als afzonderlijke challenger worden toegevoegd wanneer event-disjuncte replay
aantoont dat die de bestaande baselines verbetert.

Een dashboardstatus mag nooit preciezer of zekerder ogen dan de evidence.
Ontbrekende, stale, onvolledige of onbruikbare scans betekenen `indeterminate`
of tijdelijk `coasting`, nooit automatisch `droog` of `geen_dreiging`. De
verbetering blijft onderdeel van het dunne operationele pad en introduceert
geen nieuwe provider-, RAW-, audit-, authority- of opstartinfrastructuur.

## Aanleiding en beperkingen van de Koblenz-case

De live passage bij Koblenz van 30 september op 1 oktober 2026 leverde het
volgende regressiepunt:

- eerste voorspelling om 22:37 voor aankomst om 23:57;
- eerste latere radardetectie op de targetpositie om 00:02;
- eerste voorspelde duur circa vijf minuten tegenover circa 41 minuten
  aaneengesloten radarecho;
- 72 snapshots met `regen_op_komst` en 55 tussentijdse snapshots met
  `geen_dreiging` vóór de passage;
- twee WeatherSystem-ID's voor de gevolgde neerslagzone;
- voorspelde intensiteit van ongeveer 1,2--1,5 mm/u tegenover een latere
  eerste-bandpiek van ongeveer 1,0 mm/u in het fused radarveld;
- geen representatief, onafhankelijk grondstation met regenmeting op de
  targetpositie.

De werkelijke aankomst is niet exact 00:02. Zij ligt, op de tijdsresolutie van
de radar, tussen de laatste voorafgaande volledige droge scan en de eerste
volledige natte scan om 00:02. Exit en duur zijn op dezelfde manier
interval-gecensureerd. De case toont een bruikbaar bewegingssignaal en een
instabiele waarschuwing, maar bewijst geen algemene nauwkeurigheid. Koblenz
blijft een bevroren regressiecase en wordt nooit gebruikt om parameters te
kalibreren of releasevaardigheid te claimen.

## Ontwerpprincipes

1. Alleen strikt oplopende, geaccepteerde radar-valid-times tellen als nieuwe
   temporele evidence. Bron-, veld-, grid- en inhoudsidentiteit bepalen de
   exacte revisie van die observatie. Gewijzigde inhoud met dezelfde
   valid-time is een correctie van hetzelfde observatiemoment en telt nooit
   als een tweede support- of tegenscan.
2. Afwezig bewijs is geen bewijs van afwezigheid. Alleen een verse,
   corridor-dekkende observatie met bekende detectiekwaliteit kan een passage
   ondersteunen of weerleggen.
3. Tracking, meteorologische lineage en targetwaarschuwing zijn afzonderlijke
   verantwoordelijkheden. Een technische ID-wissel mag een passage niet
   wissen, maar een stabiele ID bewijst ook niet dat een veranderende echo één
   fysiek onveranderlijk systeem is.
4. Voor lokale neerslag zijn het volledige radarveld, de fysieke quantity,
   units, kwaliteitsmaskers en nodata leidend. Systeemgemiddelden en
   centerlines zijn alleen gelabelde fallbacks.
5. Een puntwaarde wordt uitsluitend gepubliceerd wanneer methode en evidence
   die representatie toelaten. Anders verschijnen een interval, censoring,
   scenariospread en concrete onzekerheidsredenen.
6. De twee uur is een maximale rekengrens, geen gegarandeerde skillhorizon.
   De bruikbare horizon wordt later per lead time en meteorologisch regime uit
   held-out verificatie afgeleid.
7. Replay, kalibratie en challengers draaien offline en blokkeren de
   workerstart nooit.

## 0. Vereiste correctie van het bestaande live pad

Voor nieuwe passagefunctionaliteit wordt gebouwd, moet de operationele
motion-admission correct zijn:

- `ThinRadarTracker.systems()` mag een WeatherSystem niet `motion_ready`
  noemen omdat zijn descriptieve vector alleen niet-null is;
- de initiële recursive-trackvector `(0, 0)` is initialisatie, geen gemeten
  stilstand en geen toegelaten forecastbeweging;
- systeemmotion wordt alleen toegelaten wanneer de bestaande conservatieve
  motion-consensus de dragende actuele membertracks toelaat; materiële
  onenigheid blijft fail-closed;
- de WeatherSystem-groepering krijgt fysiek begrensde, versioned
  motioncoherentie in plaats van de huidige praktisch uitgeschakelde grens;
- split-, merge- en identity-alternative-evidence uit de bestaande
  CellTrackEngine wordt binnen de bestaande resourcegrenzen geactiveerd of
  expliciet als onbekend gemarkeerd.

De bestaande CellTrackEngine en WeatherSystem-groepering blijven de enige
tracking- en groepsautoriteit. Er komt geen tweede globale assignment- of
tracking-engine.

Het dunne veldcontract wordt vóór forecasting uitgebreid met minimaal
`quantity`, `unit`, quality/mask-semantiek, processing version,
source composition, canonical valid-time, `received_at`/`available_at`,
grid-ID en content fingerprint. De huidige impliciete hardcodering van ieder
veld als `rain_rate/mm/h` vervalt. Al deze eigenschappen worden ongewijzigd
naar tracking, forecast, opslag en verificatie doorgegeven.

## 1. Lineage en targetgebonden passagecontinuïteit

WeatherSystem gebruikt bestaande membertracks, predecessor candidates en
membership events om identiteit over opeenvolgende geaccepteerde scans door te
geven. Splits, merges en ambigue opvolging blijven expliciete lineage-events.
Een lege, onvolledige of geweigerde scan verbreekt lineage niet stil.

Boven die bestaande lineage komt uitsluitend een kleine, begrensde
passage-state-registry per target. Zij doet geen nieuwe globale objectmatching.
Een entry verwijst naar:

- een stabiele `passage_system_id` als continuïteitsalias;
- de onderliggende WeatherSystem- en CellTrack-ID's plus lineage-events;
- targetpositie, position accuracy en actieve region revision;
- laatst ondersteunde forecast, supportframes en statusovergangen.

De alias wordt alleen overgedragen wanneer bestaande auditable lineage of
expliciete geadveceerde-footprintoverlap dat ondersteunt. Bij ambiguïteit blijft
de identiteit `ambiguous`; zij wordt niet automatisch aan de dichtstbijzijnde
cel gekoppeld. Bij splitsing kunnen meerdere passages uit één parent ontstaan.
Bij merge behouden alle parent-ID's provenance terwijl één nieuwe primaire
passagealias wordt gekozen volgens een versioned, deterministische regel.

Na een workerrestart wordt in-memory passage-evidence niet als actueel
voorgesteld. De worker start onmiddellijk, publiceert `recovering/indeterminate`
en bouwt state opnieuw op uit nieuwe unieke scans. Historische issue-time-
voorspellingen blijven wel bewaard voor verificatie; zij worden niet opnieuw
als actuele forecast gebruikt. PostGIS-herstel blokkeert de start niet.

## 2. Unieke velden en bewegingsbewijs

De runtime bewaart een begrensde ring van geaccepteerde radarvelden,
validiteitsmaskers, broncompositie, grididentiteit en bijbehorende
motion-evidence. De exacte capaciteit en tijdspanne zijn versioned operationele
parameters en worden gekozen op basis van resourceproeven en multi-event
replay. `zes velden / dertig minuten` mag als eerste kandidaat worden getest,
maar is geen wetenschappelijk vastgestelde grens.

Een motionpaar vereist fysiek vergelijkbare opeenvolgende velden: dezelfde of
expliciet geharmoniseerde quantity, unit, grid, kalibratie en verwerking, en
geen materiële wijziging van broncompositie binnen het gebruikte gebied. Bij
een compositiewissel wordt motion uit een aantoonbaar gemeenschappelijke
providerbasis afgeleid of blijft zij `indeterminate`; gelijke pixelsizes alleen
zijn onvoldoende.

De centrale beweging komt uit de bestaande conservatieve motion-consensus.
Optical flow en objecttracking gebruiken dezelfde radarbeelden en gelden dus
nooit als onafhankelijke metingen. De eerste baseline gebruikt per
WeatherSystem één toegelaten consensusvector nadat alle dragende actuele
membervectoren door een fail-closed coherentiepoort zijn gegaan. Een niet-null
descriptief systeemmediaan zonder die admission volstaat niet.

Een echt ruimtelijk variërend optical-flowveld bestaat nog niet als toegelaten
operationeel contract. Het wordt daarom een afzonderlijke challenger. Voor
promotie moet dat contract per gridpunt vector, validiteitsmasker, support,
coherentie, extrapolatie buiten geldige flow en provenance vastleggen en in
event-disjuncte replay beter presteren dan de uniforme-vectorbaseline.

Er wordt geen nieuwe harde `3 x 1.4826 x MAD`-poort toegepast. Met weinig,
gecorreleerde 2D-vectoren levert die regel geen gekalibreerde uitbijtertest en
faalt zij onder meer voor residuals `[0, 0, D]`. Ruwe kandidaten, covariantie,
support, verschillen en afwijzingsredenen blijven als provenance beschikbaar;
materiële onenigheid geeft `indeterminate`.

Twee unieke geschikte scans kunnen een voorlopige constant-motion-schatting
opleveren. Dat bewijst geen groei, verval of betrouwbare eindtijd. Meerdere
historische bewegingen mogen alleen als deterministische
gevoeligheidsscenario's worden gebruikt wanneer zij ieder afzonderlijk door de
admission komen. Hun aantal maakt ze niet onafhankelijk en vormt geen ensemble.

## 3. Gelabelde veldadvectie en passage-intervallen

De primaire baseline neemt uit het nieuwste volledige kwantitatieve veld de
QPE-pixels en het bronlabelmasker van ieder forecastbaar WeatherSystem. Zij
advecteert die gelabelde footprint en zijn waarden rigide met de toegelaten
systeem-consensusvector. Dit is een objectgebonden Lagrangian-
persistencebaseline: het volledige lokale intensiteitspatroon blijft bewaard,
maar shear, rotatie, groei en verval worden nog niet voorspeld. Een centerline
met vaste straal is de laatste fallback en publiceert geen nauwkeurige duur of
lokale intensiteit.

Het bronlabelmasker beweegt met het QPE-veld mee. Daardoor blijft ieder nat
interval gekoppeld aan zijn WeatherSystem-lineage. Bij overlappende of
samensmeltende geadveceerde maskers blijven afzonderlijke kandidaten bestaan;
wanneer toewijzing niet eenduidig is, krijgt de passage expliciet
`lineage_ambiguous` in plaats van een verzonnen systeem-ID. QPE-pixels zonder
bruikbaar systeemlabel mogen een veldwaarschuwing geven, maar geen stabiele
stormidentiteit claimen.

Per target wordt op iedere forecast-valid-time de target-support bepaald uit
targetpositie, position accuracy, gridresolutie, dekking en nodata. Een passage
is een maximaal aaneengesloten tijdinterval waarin het geadveceerde veld de
versioned nat-drempel overschrijdt. Een droge onderbreking maakt een nieuw
interval; eerste entry en laatste exit worden nooit over meerdere losse
passages samengevouwen.

Bij discrete forecasttijden worden entry en exit als brackets bewaard: laatste
droge tijd versus eerste natte tijd voor entry, en laatste natte tijd versus
eerste droge tijd voor exit. Wanneer het target bij issue-time al nat is, is de
entry `left_censored` en wordt geen normale aankomstbracket verzonnen. Een
centraal tijdstip mag als schatting worden getoond, maar blijft als schatting
gelabeld. Een exit buiten de rekenhorizon is `right_censored`; het
horizonmoment wordt niet als voorspeld einde getoond.

Voor ieder toegelaten gevoeligheidsscenario worden als één gekoppeld resultaat
bewaard:

- hit, miss of horizon-censored;
- entrybracket, exitbracket en passage-identiteit;
- lokale intensiteitssamenvatting en gebruikte field-ID;
- motionmethode, support, source/grid-compositie en redenencodes.

De samenvatting rapporteert altijd de totale denominator en afzonderlijke
aantallen hit-, miss-, left-censored- en right-censored-scenario's.
Entry-min/max gebruikt alleen hits met een finite entrybracket; exit-min/max
alleen hits met een finite, niet-right-censored exitbracket. De gekoppelde
entry/exit van ieder individueel scenario blijft bewaard zodat geen
onmogelijke combinatie van twee verschillende scenario's als duur verschijnt.
`sampled_motion_min/max` wordt uitsluitend over deze benoemde deelverzamelingen
berekend.
Dit is een beschrijvende bewegingsspreiding, geen confidence- of
voorspellingsinterval. Een nulbrede spread krijgt geen exact ogende UI-status.
Een centraal entry- of exitmoment wordt alleen gepubliceerd wanneer een
versioned admission policy voldoende consistente hits, dekking en support
vereist; anders blijft het resultaat voorlopig of `indeterminate`.

Formele passagekansen en quantielen mogen pas via het bestaande
`TrackerForecast`-contract worden gepubliceerd na geldige ensemblefrequenties
of empirische, event-disjuncte kalibratie.

## 4. Evidencegestuurde passage-status

Radar-gedreven bevestigings- en annuleringstellers verwerken uitsluitend
geaccepteerde scans met strikt oplopende valid-time. De actuele
neerslagstatus mag daarnaast veranderen door een nieuwe, representatieve
grondobservatie; zo'n grondobservatie verhoogt geen radarscanteller.

De passage-statussen zijn:

- `candidate`: minimaal één geldige projectie ondersteunt een passage;
- `confirmed`: de versioned hysteresispolicy heeft voldoende opeenvolgende
  support; deze status betekent operationele bevestiging, geen statistische
  confidence;
- `coasting`: een eerder bevestigde passage mist tijdelijk bruikbare nieuwe
  evidence zonder geldige weerlegging;
- `current`: een verse, volledige en voor near-surface precipitation
  gevalideerde radar/QPE-observatie of representatieve onafhankelijke lokale
  meting toont neerslag op de target-support;
- `ended`: verse evidence bracket de exit van de passage;
- `cancelled`: de versioned hysteresispolicy heeft voldoende opeenvolgende,
  volledige en corridor-dekkende tegenobservaties;
- `indeterminate`: evidence is te oud, onvolledig, tegenstrijdig of buiten de
  aantoonbare skill-/rekengrens.

Een eerste beleidsversie mag twee opeenvolgende supportscans voor `confirmed`
en twee opeenvolgende volledige tegenobservaties voor `cancelled` gebruiken,
maar deze aantallen zijn preregistreerde kandidaten. Zij moeten met multi-event
replay en prospectieve data worden beoordeeld op miss, vals alarm,
false-cancel en cancel-naar-realert. Zij zijn geen wetenschappelijke
zekerheidsgrens.

Een corridor-dekkende tegenobservatie is passage- en lineagegebonden. Zij moet
de actuele footprint van dezelfde lineage én het volledige gebied dekken
waarover die footprint volgens iedere nog toegelaten beweging het target
binnen het resterende forecastvenster zou kunnen bereiken. Na actualisatie met
die scan mag geen toegelaten projectie van die lineage het target nog raken.
Alleen constateren dat het target op dat moment droog is, weerlegt een latere
verwachte aankomst niet. Wanneer lineage, corridordekking of projectie
onbeslist blijft, wordt niet geannuleerd maar blijft de status `coasting` of
`indeterminate`.

Stale, herhaalde, nodata-, grid-incompatibele of niet-corridor-dekkende frames
tellen nooit mee voor bevestiging of annulering. Een willekeurige andere
motion-ready cel die het target mist, weerlegt een bestaande passage niet.
`coasting` vergroot niet willekeurig een tijdvenster en blijft niet onbeperkt
bestaan. Voor validatie bestaat er nog geen aangetoonde skillhorizon; daarom
wordt `candidate_policy_v1` live uitsluitend als `provisional_unvalidated`
getoond binnen zijn expliciete kandidaat-rekenhorizon van maximaal twee uur.
Coasting stopt uiterlijk bij de oorspronkelijke horizon of wanneer de
versioned freshnesslimiet wordt overschreden. Na voldoende held-out bewijs
vervangt de gemeten skillhorizon deze kandidaatgrens; daarbuiten wordt de
status `indeterminate`.

Wanneer meerdere systemen het target kunnen raken, blijven hun passages en
eventuele re-entry's afzonderlijk en chronologisch gesorteerd. De operationele
samenvatting kiest de eerstvolgende toegelaten passage zonder latere passages
uit PostGIS of het dashboard te verwijderen.

## 5. Lokale intensiteit en quantity-gating

Een numeriek veld is alleen voor lokale intensiteit geschikt wanneer zijn
quantity expliciet neerslagintensiteit/QPE voorstelt, de eenheid compatibel is
met `mm/h`, quality en nodata geldig zijn en het veld voor de target-support
dekking biedt. Een DBZH-reflectiviteitsveld, zoals de huidige OPERA DBZH-
decoder, mag echo en beweging ondersteunen maar levert zonder afzonderlijk
gevalideerde conversie geen `mm/h`. Een reflectiviteitsecho alleen heet
`echo_detected`, niet automatisch `raining`; alleen een expliciet voor
near-surface precipitation occurrence gevalideerd product mag zonder QPE een
nat/droogstatus of `echo_passage` ondersteunen. Een `echo_passage` blijft
onderscheiden van een voorspelde regenpassage op de grond.

De baseline advecteert de laatste geldige QPE-waarden onder
intensity-persistence. Per aaneengesloten passage worden minimaal bewaard:

- tijdgewogen lokale gemiddelde radarintensiteit;
- lokaal gesampled piektempo op de gebruikte forecasttijdstappen;
- entry-/exitbrackets en de exacte tijdsupport van beide samenvattingen;
- field-, quantity-, unit-, quality-, valid-time- en nodataprovenance;
- spreiding over toegelaten bewegingsscenario's, afzonderlijk van QPE- en
  groei/vervalonzekerheid.

Deze waarden heten `advected_radar_estimated_rain_rate`; zij zijn geen
gekalibreerde grondmeting. Wanneer geen geschikt QPE-raster beschikbaar is,
kan de footprintpassage bruikbaar blijven terwijl intensiteit `unknown` is.
Systeemmaximum of systeemgemiddelde mag alleen diagnostisch en expliciet als
niet-lokale fallback worden getoond.

## 6. Opslag, issue-time-integriteit en presentatiecontracten

De bestaande `stv3_target_passage`, `stv3_target_forecast`,
`stv3_storm_cell`, `stv3_track_point` en fusiongegevens blijven de basis. De
wijziging vereist wel een gecoördineerde contractversie:

- voorlopige, coasting-, cancelled-, indeterminate- en gecensureerde forecasts
  komen in versioned `forecast_metadata` bij `stv3_target_forecast`;
- `stv3_target_passage` blijft uitsluitend een volledig geometrisch
  passage-resultaat met bruikbare entry- en exitrepresentatie; een voorlopige
  aankomst krijgt geen verzonnen `exit_at` om aan het bestaande non-nullcontract
  te voldoen;
- iedere issue-time-voorspelling krijgt een immutable `forecast_issue_id`,
  afgeleid van target, issued-at, algorithm/policy version, region revision en
  inputfingerprints. Iedere losse passage of re-entry krijgt een immutable
  `passage_id`, afgeleid van issue-ID, lineagealias en intervalordinal;
- een idempotente migratie maakt deze ID's onderdeel van de opslagidentiteit,
  zodat meerdere algorithmversies op één issue-time en meerdere intervallen
  van hetzelfde systeem niet via de bestaande upsertkeys worden overschreven;
- lineage, scenario-uitkomsten, censoring, supportframes en quantity-provenance
  krijgen een expliciet JSON-schema en groottebegrenzing;
- MQTT, het strikte operational-snapshot-schema en de forecastkaart worden in
  dezelfde releaseversie aangepast. Nieuwe keys of statussen worden niet stil
  aan een exact-keycontract toegevoegd.

De gebruikersweergave maakt onderscheid tussen `voorlopig`, `bevestigd`,
`coasting/onzeker`, `actueel`, `afgelopen`, `geannuleerd` en `onbepaald`.
Geschatte tijden, brackets, sampled spread en horizon-censoring krijgen ieder
hun eigen label. Een technische cel-ID blijft diagnostische provenance.

Een PostGIS-storing verhindert de actuele MQTT-publicatie niet. Replay,
historieherstel en verificatie worden geen workerstartvoorwaarde.

## 7. Verificatie en kalibratie

### 7.1 Temporale radarreferentie

Een issue-time-forecast wordt bevroren met alle input-valid-times,
`available_at`/`received_at` en fingerprints. Iedere input moet aantoonbaar
uiterlijk op `issued_at` beschikbaar zijn; offline replay gebruikt exact de
toen beschikbare revisie. Een veld met een oude valid-time dat pas later werd
ontvangen mag nooit retrospectief in de issue-time-input verschijnen. Alleen
strikt latere, volledige radar-valid-times mogen de voorspelling achteraf
beoordelen. Latere Storm Tracker-fusie heet een
`temporal_radar_reference`, geen onafhankelijke grondwaarheid. Exacte
providercompositie, algoritmeversie, grid, quantity, threshold, maskers en
dekking worden bewaard zodat sourcewissels en leakage zichtbaar blijven.

Radar-entry en -exit worden als tijdsbrackets beoordeeld. Errorscores gebruiken
die brackets of rapporteren expliciet de resolutiegebonden onzekerheid. Een
fused radarproduct van Storm Tracker wordt nooit als extern onafhankelijke
truth aan de bestaande formele forecast-verifier aangeboden.

### 7.2 Onafhankelijke grondvalidatie

Een grondmeting is alleen extern onafhankelijke validatie wanneer zij:

- representatief en voldoende collocated is voor de target en het doel;
- een gecontroleerde regenquantity met passende unit en accumulatieperiode
  levert;
- temporeel met de forecast- en passagebrackets overeenkomt;
- niet opnieuw als input in het te beoordelen fused radarproduct is gebruikt.

Een willekeurig station binnen 50 km voldoet hier niet automatisch aan.
Passagegemiddelde en piek worden uitsluitend met dezelfde observatiegrootheid
over hetzelfde tijdvenster vergeleken.

### 7.3 Baselines, splits en releasebewijs

De kandidaat wordt gepaard vergeleken met de bestaande Eulerian-persistence-
en tweebeeld-Lagrangian/constant-motion-baselines. Events worden vooraf
leakage-safe gegroepeerd en vervolgens gescheiden in ontwikkeling, validatie en
onaangeraakte test. Koblenz blijft alleen regressie/development.

Per onafhankelijk event en per vooraf geregistreerde lead-, provider-, regio-
en stormtypestratum worden minimaal gerapporteerd:

- aantallen natte en droge forecast opportunities;
- hit, miss, vals alarm, false-cancel en cancel-naar-realert;
- waarschuwingstijd en continuïteit;
- bracket-aware entry-, exit- en duurfout;
- sampled-motion-spread, hit/miss/censored-scenarioverdeling en twee vooraf
  gedefinieerde overlapmaten: `bracket_intersection` wanneer radartruthbracket
  en sampled range elkaar raken, en de strengere `bracket_containment` wanneer
  de sampled range de volledige radartruthbracket bevat. Geen van beide maakt
  de spread tot een confidence-interval;
- lokale gemiddelde- en piekfout op exact gematchte tijdvensters;
- identity switches, coastingframes, splits, merges en onbekende uitkomsten.

Aggregatie en bootstrap gebeuren per meteorologisch event, niet per frame.
Policykandidaten, truthdrempels, sample-sizebesluiten en releasemarges worden
voor de onaangeraakte test vastgelegd. Een challenger promoveert alleen bij
reproduceerbare verbetering tegenover de baselines zonder onaanvaardbare
verslechtering in miss- of vals-alarmgedrag. Daarna volgt prospectieve
monitoring; er wordt niet op de held-out test hergetuned.

## Versioned kandidaatparameters

De eerste implementatie mag onderstaande waarden uitsluitend onder een
duidelijke `candidate_policy_v1` gebruiken:

- voorlopige motion na minimaal twee geschikte scans met strikt oplopende
  valid-time;
- bevestiging na twee opeenvolgende geschikte supportscans;
- annulering na twee opeenvolgende volledige corridor-dekkende tegenscans;
- een bounded field/evidence ring met als te toetsen startkandidaat maximaal
  zes velden en dertig minuten historie.

Deze waarden bepalen state-machinehysteresis en resourcegebruik. Zij betekenen
geen confidencepercentage en worden pas release-defaults nadat de beschreven
replay- en prospectieve checks zijn doorlopen. Er geldt geen universele regel
dat drie scans automatisch een betrouwbare exit of scenario-range opleveren.

## Foutafhandeling

- Minder dan twee geschikte unieke scans geeft geen bewegingsvoorspelling.
- Een forecast met slechts één toegelaten bewegingsscenario krijgt geen
  sampled-motion-spread.
- Scenario's die het target missen of alleen na de horizon raken worden niet
  uit de denominator verwijderd.
- Een horizon-afgekapt vertrek blijft right-censored en wordt geen eindtijd.
- Een correctie op dezelfde valid-time vervangt of annoteert dezelfde
  observatierevisie en telt nooit als nieuwe temporele evidence.
- Een analysis-grid-, processing-, composition- of quantitywijziging sluit
  oude en nieuwe velden niet stil op elkaar aan; incompatibele evidence wordt
  afgewezen of expliciet getransformeerd met provenance.
- Nodata, buiten-grid en ontbrekende corridordekking worden nooit als nulregen
  of tegenbewijs geïnterpreteerd.
- Een workerrestart levert eerst `recovering/indeterminate`, niet een opnieuw
  actueel verklaarde oude voorspelling.
- Een PostGIS-storing verhindert de actuele MQTT-publicatie niet.
- Replay, verificatie en kalibratie draaien nooit tijdens workerstart.

## Acceptatiecriteria voor implementatie

1. WeatherSystem-motion wordt niet `ready` door alleen een non-null of
   geïnitialiseerde nulvector; de conservatieve consensus beslist admission.
2. Fysiek incoherente membertracks worden niet stil één forecastbaar systeem;
   split, merge en ambiguïteit blijven auditable.
3. Geen enkele herhaalde valid-time verandert een bevestigings-, coasting- of
   annuleringsteller, ook niet wanneer inhoudsfingerprint of providercompositie
   werd gecorrigeerd.
4. Een ontbrekende, stale, nodata- of niet-corridor-dekkende scan annuleert geen
   passage en veroorzaakt niet automatisch `geen_dreiging`.
5. Tegenbewijs telt alleen bij volledige, verse, quantity- en
   kwaliteitsgeschikte corridordekking volgens de versioned policy.
6. Meerdere passages en re-entry's blijven afzonderlijk, chronologisch en met
   eigen lineage bewaard.
7. De primaire lokale forecast advecteert quantity-correcte QPE-pixels samen
   met hun systeemlabelmasker; DBZH of systeemgemiddelden worden nooit als
   lokale `mm/h` voorgesteld.
8. Voorlopige aankomst kan zonder verzonnen vertrek worden gepubliceerd.
   Exit, duur en horizon-censoring blijven semantisch onderscheiden.
9. Scenariohit, -miss en -censoring blijven zichtbaar; drie frames of een
   nulbrede spread leveren geen schijnzekerheid.
10. Koblenz-replay bevat geen future-data-leakage, bewaart alle issue-time-
    voorspellingen, maakt de radarbrackets zichtbaar en reproduceert geen
    ongerechtvaardigde statewissel door één ontbrekend beeld.
11. Synthetische tests omvatten dubbele valid-time, `[0, 0, D]`-vectoren,
    correlated motion, rechte beweging, versnelling, vervorming, groei/verval,
    dropout, nodata, split, merge, twee passages, re-entry, horizoncensoring en
    een echte miss. Niet-gemodelleerde groei/verval moet eerlijk degraderen of
    `indeterminate` worden, niet kunstmatig correct lijken.
12. De formele external-truth-verifier weigert Storm Tracker's eigen fused
    radarproduct als onafhankelijke waarheid.
13. Een replay weigert ieder issue-time-input met `available_at` of
    `received_at` na `issued_at` en bewaart de exacte as-of-revisie.
14. MQTT-, snapshot-, dashboard- en PostGIS-contracttests bewijzen dezelfde
    statussen, tijden, brackets, censoring en units end-to-end.
15. Bestaande providerophaling, onmiddellijke workerstart, PostGIS-retentie en
    radarweergave krijgen geen nieuwe audit- of opstartafhankelijkheid.

## Uitgesloten uit deze wijziging

- automatisch herwegen van providers op basis van één of enkele events;
- een nieuwe stationimporter of grondstationcatalogus;
- pySTEPS als verplichte runtime;
- meldingen naar de NUC-HA;
- een derde tracking-, assignment- of lineage-engine;
- nieuwe RAW-, manifest-, authority-, ledger- of opstartaudits;
- een ongevalideerd groei-/vervalmodel;
- cosmetische timers of confidencepercentages zonder meteorologische evidence.

## Wetenschappelijke referenties

- WMO, *Nowcasting Guidelines -- A Summary*:
  https://public.wmo.int/media/magazine-article/nowcasting-guidelines-summary
- Pulkkinen et al. (2019), *Pysteps: an open-source Python library for
  probabilistic precipitation nowcasting*, GMD 12, 4185--4219:
  https://gmd.copernicus.org/articles/12/4185/2019/
- Lakshmanan et al. (2009), *An Improved Method for Estimating Radar Echo
  Motion*, Journal of Atmospheric and Oceanic Technology:
  https://journals.ametsoc.org/doi/pdf/10.1175/2008JTECHA1084.1
- NIST, *Median Absolute Deviation*:
  https://itl.nist.gov/div898/software/dataplot/refman2/auxillar/mad.htm
