# P1-T04 — WeatherSystem als secundaire groepering

Status: pure lokale shadowcode, exact gespiegeld voor Home Assistant en worker.
Niet live geïmporteerd, niet gedeployed en niet gekoppeld aan de legacy
`StormEngine`.

Implementatie:

- `custom_components/storm_tracker_v3/engine/weather_system_grouping.py`;
- `storm_tracker_worker/tracking_core/engine/weather_system_grouping.py`;
- `custom_components/storm_tracker_v3/engine/meteorological_evidence_store.py`;
- `storm_tracker_worker/tracking_core/engine/meteorological_evidence_store.py`;
- `tests/test_weather_system_grouping.py`;
- `tests/test_meteorological_evidence_store.py`.

## 1. Architectuurgrens

Een `WeatherSystem` is uitsluitend een actuele bovenliggende view op
`CellTrack`-snapshots:

- iedere inputtrack blijft exact eenmaal bestaan;
- geen detection, track of lineage wordt gemerged, verwijderd of hernoemd;
- lokale celpositie, lokale celmotion en lifecycle blijven eigendom van de
  CellTrack-laag;
- system motion is alleen beschrijvende output;
- de module bevat geen methode om system motion terug te schrijven;
- absolute snelheid wordt nergens begrensd of als natuurwet gebruikt;
- een onzekere verbinding wordt bewaard, niet stil verworpen.

De output embedt de volledige immutable `WeatherSystemTrackInput` van ieder
member. Daardoor blijven parent-/childtrack-ID’s, lineage-event-ID’s en
identityalternatieven ook bij systeemmerge en membershipwijziging aanwezig.

## 2. Actuele input

`WeatherSystemTrackInput` bevat minimaal:

- stabiele `track_id` en actuele `track_snapshot_id`;
- exact analysis grid en exact groupingtijdstip;
- actuele detection en objecttype (`rain_shield` of `embedded_core`);
- de actuele footprint als gesorteerde pixelset;
- actuele rain-shield-detection-ID’s;
- lokale positie;
- optionele volledige lokale motionvector;
- motionreferentietijd en motionmethode;
- lifecycle;
- parent-, child-, lineage- en identityalternative-ID’s.

Alle footprints in één run moeten op exact hetzelfde metrische `AnalysisGrid`
en exact hetzelfde `valid_at` liggen. Dit is bewust streng: anders zou een
zogenoemde actuele union in werkelijkheid oude en nieuwe footprints mengen.
Motionreferentietijden mogen verschillen, maar hun leeftijd en onderlinge
tijdsverschil zijn expliciete evidence.

Een footprintpixel buiten het grid, dubbele track-/snapshot-/detection-ID,
future motionreferentie of gedeeltelijke vector faalt het pure inputcontract.
Er wordt daarbij geen live track aangepast.

## 3. Caller-owned policy

`WeatherSystemGroupingPolicy` heeft geen productdefaults. De caller moet
expliciet vastleggen:

- 4- of 8-pixelconnectiviteit;
- maximale actuele footprintafstand;
- maximale motionvectordifferentie;
- maximale motionleeftijd;
- maximaal verschil tussen motionreferentietijden;
- policy-ID.

Deze waarden zijn groeperingscriteria, geen observation- of speedfilters. Een
track met een extreme maar eindige snelheid blijft volledig in de output.
Policyvelden en algoritmeversie vormen samen een fingerprint die in connection-,
system- en groupingsnapshot-ID’s doorwerkt.

## 4. Pairwise connection evidence

Voor ieder trackpaar wordt exact één
`WeatherSystemConnectionEvidence` bewaard. Bij `n` tracks zijn dat
`n × (n - 1) / 2` records; er is geen objectcountcap.

Per paar staan minimaal:

- gedeelde actuele rain-shield-ID’s;
- overlap in pixels;
- exacte afstand tussen de buitenranden van actuele footprintpixels;
- verschil in actuele geldigheidstijd;
- verschil tussen motionreferentietijden;
- motionvectordifferentie;
- motionstatus `coherent`, `incoherent`, `stale` of `unknown`;
- ruimtelijke status `shared_rain_shield`, `overlap`, `touching`, `near` of
  `far`;
- beslissing `connected`, `uncertain` of `separate`;
- deterministische redenen.

Beslisregels:

1. Een expliciet gedeeld actueel rain shield is een sterke verbinding.
   Incoherente lokale puppybeweging verbreekt die hiërarchische relatie niet,
   maar het conflict blijft in pair- en systemuncertainty staan.
2. Overlap, aanraking of toegelaten actuele afstand verbindt alleen wanneer de
   motion ook tijdig en coherent is.
3. Ruimtelijk plausibel met ontbrekende, stale of incoherente motion wordt
   `uncertain` en vormt geen stil zekere component.
4. Te verre actuele footprints blijven afzonderlijk.

Connected edges vormen deterministisch de componenten. Uncertain edges blijven
als evidence bij beide betrokken systemen zichtbaar.

## 5. Actuele system footprint

`aggregate_current_footprint` is exact:

```text
sorted(set(pixel for current member for pixel in member.current_footprint))
```

Er wordt geen convex hull berekend. Oude footprints uit een vorige
groupingsnapshot worden niet meegenomen. Een track die van pixelgebied A naar B
verplaatst, levert in het nieuwe snapshot alleen B; het gebied tussen A en B
wordt niet kunstmatig gevuld.

Omdat alle members hetzelfde grid en tijdstip moeten delen, blijft deze union
geometrisch en temporeel eenduidig.

## 6. Gecombineerde hondenlijn

De beschrijvende systeemmotion gebruikt:

1. actuele rain-shieldtrackmotions indien beschikbaar;
2. anders actuele overige membermotions;
3. anders expliciet `None`.

De vector is de deterministische componentgewijze mediaan. De contributing
track-ID’s en maximale memberresidual blijven zichtbaar. Embedded-coremotions
worden dus niet gebruikt om een aanwezige centrale rain-shieldmotion te
verplaatsen.

Hierdoor kunnen puppies intern snel, tegengesteld of chaotisch bewegen terwijl
de system motion dezelfde centrale rain-shieldbeweging blijft beschrijven.
Hun eigen vectors blijven onaangeroerd in de memberinputs.

Deze beschrijvende mediaan is geen bewezen forecastmodel en geen toestemming
om hem naar CellTracks terug te schrijven.

## 7. System identity en membershiplineage

Bij een eerste snapshot wordt een deterministisch `system_id` uit grid en
rain-shieldtrack-ID’s afgeleid. Bij een volgend snapshot worden alle mogelijke
predecessors bewaard op basis van:

- direct overlevende membertrack-ID’s;
- actuele parenttracklinks;
- expliciete identityalternative-tracklinks.

De selectie is deterministisch en één-op-één. Alle niet-geselecteerde
predecessorcandidates blijven als alternatieven staan.

Membershipevents onderscheiden:

- `birth`;
- `continued`;
- `membership_changed`;
- `merge`;
- `split_retained`;
- `split_child`;
- `complex_reconfiguration`.

Bij een systeemmerge blijft één predecessor-ID geselecteerd; alle andere
predecessors en hun membertracks blijven expliciet. Bij een systeemsplit kan
slechts één child de oude system-ID behouden. Andere children krijgen een
nieuwe collision-safe ID en bewaren dezelfde predecessor als niet-geselecteerde
lineage. Geen CellTrack-lineage wordt daarbij geërfd, samengevouwen of gewist.

`retired_system_ids` beschrijft alleen bovenliggende systeemidentiteiten die
niet werden voortgezet. Het verwijdert geen track.

## 8. Determinisme en invarianten

De constructie controleert:

- inputvolgorde verandert geen component, ID of snapshot;
- ieder inputtrack-ID komt exact eenmaal in de systemmembers voor;
- iedere trackpair heeft exact één connectionrecord;
- system-, candidate- en track-ID’s zijn uniek;
- ieder systeem heeft exact één passend membershipevent;
- aggregate footprint en rain-shield-ID’s zijn exacte actuele unions;
- policy en algoritmeversie zijn volledig aanwezig;
- classification blijft `not_classified`; de groepering verzint geen MCS.

## 9. Tests

De tests dekken onder andere:

- chaotische puppybeweging onder één rain shield zonder systemsprong;
- onveranderde centrale descriptive motion bij puppyrearrangement;
- extreme snelheden zonder cap/reject;
- actuele overlap/afstand met coherent, incoherent, stale en unknown motion;
- expliciete uncertain connections;
- actuele union zonder historische convex hull;
- systeemmerge en -split met alle lineage/alternatieven;
- collision-safe system-ID bij split;
- meer dan twintig tracks zonder verlies;
- willekeurige inputreordering;
- leeg actueel frame;
- strikte tijd-, grid- en motionprovenance;
- byte-identieke HA-/workermirrors;
- geen live import in integration setup, sensor of workerservice.

## 10. Durable shadowevidence

Schema v3 van de geïsoleerde meteorologische evidence-store bewaart één
groupingsnapshot als één SQLite-transactie. De parent bevat de volledige
canonieke snapshot-JSON; genormaliseerde append-only childtabellen bewaren
daarnaast:

- de volledige caller-owned policy en reproduceerbare fingerprint;
- iedere inputtrack met snapshot, detectie, footprint, lokale motion,
  lifecycle, parent/child-ID's, CellTrack-lineage en identityalternatieven;
- ieder systeem met de exacte actuele footprintunion;
- ieder afzonderlijk footprintpixel;
- alle membertracks en alle bronnen van de beschrijvende motion;
- iedere pairwise `connected`/`uncertain`/`separate`-beslissing met redenen;
- alle predecessorcandidates, inclusief de geselecteerde en alternatieven;
- ieder membership-/merge-/split-/reconfiguratie-event.

Voor opslag wordt gecontroleerd dat track, snapshot, actuele detectie,
geldigheidstijd, lifecycle en analysis grid overeenkomen met de reeds
append-only opgeslagen CellTrack-evidence. Een ontbrekende of semantisch
verkeerde basisreferentie faalt vóór de groupingbundle wordt geschreven.

Exact dezelfde grouping opnieuw aanbieden is idempotent. Dezelfde logische
grouping-ID met andere immutable inhoud wordt als conflict geregistreerd
zonder een gedeeltelijke childrevision te schrijven. `load_weather_system_grouping`
controleert de parentidentiteit, canonieke JSON en alle genormaliseerde
childsets; ontbrekende footprint-, member-, motion-, link-, predecessor- of
eventevidence faalt gesloten.

Een bestaande exacte schema-v2-evidence-ledger wordt transactioneel naar v3
gemigreerd. De migratie accepteert alleen de volledige verwachte v2-tabellen,
kolommen, append-only triggers, store-identiteit, migratiehistorie, geldige
foreign keys en een geslaagde SQLite-integriteitscontrole. Onbekende of
beschadigde v2-databases worden niet gedeeltelijk aangepast. Een nieuwe lege
database krijgt rechtstreeks v3.

Dit is nog steeds shadowcode: er is geen live import, providerwrite,
workerjob, deploy of databasewijziging uitgevoerd.

## 11. Open beperkingen

Deze shadowlaag maakt nog geen meteorologische skillclaim:

- policygrenzen zijn niet op event-held-out data gekalibreerd;
- de componentgewijze median motion is uitsluitend beschrijvend;
- rain-shield-ID’s moeten correct door de detector-/tracklaag worden geleverd;
- de pure pairwise evaluatie is kwadratisch in het trackaantal;
- exacte footprintafstand vergelijkt pixelparen en vraagt later een
  ruimtelijke index voor grote rasters/footprints;
- MCS-classificatie is bewust niet geïmplementeerd;
- de durable evidence-store is lokaal geïmplementeerd maar nog niet live
  gewired; nowcast-, ground- en dashboardwiring ontbreken eveneens;
- live shadowsoak en NUC-runtime-/geheugenmetingen zijn nog nodig.
