# Read-only observatie: brede regenzone België, 2026-07-30

Status: operationele momentopname uit Home Assistant; uitsluitend read-only
uitgelezen. Er is niets gewijzigd, gedeployed of herstart.

## Waarnemingsvenster

De sensoren zijn uitgelezen rond 14:10–14:12 CEST op 2026-07-30. Deze notitie
is een auditmomentopname, geen immutable export van de onderliggende
providerartifacts en geen onafhankelijke meteorologische waarheid.

## Regio en bronnen

- `region-1` lag rond 51,0498° N, 4,4186° E met een observatieradius van
  250 km.
- De actief gekozen operationele radarbron was KMI, met als opgegeven reden
  `lokale officiële radar voor BE`.
- `sensor.stv3_kmi_observaties` meldde `neerslag`, 88 rasterobservaties,
  gemiddelde interne intensiteitswaarde 1,3 en maximum 4.
- Het laatste gemelde KMI-frame was geldig om 12:10 UTC.
- OPERA had volledige dekking van de 250-km-regio en meldde 124 ruwe en
  103 geaccepteerde objectobservaties voor het product van 12:05 UTC.
- De OPERA-segmentatie bevatte één oudercomponent van circa 97.422 km² met
  vele ingesloten subcellen/cores. Dit ondersteunt de gebruikerswaarneming van
  een brede regenzone, maar is geen onafhankelijk gevalideerde systeemgrens.
- RainViewer leverde gelijktijdig aanvullende observaties, maar KMI bleef de
  operationeel gekozen bron voor België.

## Wat de live motionlaag meldde

Voor `region-1` gebruikte de motion-preflight drie frames en rapporteerde:

- status `unstable_motion`;
- confidence `low`;
- gewone translation: circa 50,8 km/u;
- centroidtranslation: circa 346,9 km/u;
- motion-match-IoU circa 0,254;
- verificatie-IoU circa 0,245;
- nowcaststatus `insufficient_motion`;
- de experimentele motionlaag voedde de detectie niet.

Dit is intern tegenstrijdig bewijs. Het mag niet worden samengevat tot één
zekere snelheid en zegt niets negatiefs over de intrinsieke kwaliteit van het
KMI-product. Het toont dat een brede groeiende/krimpende echo geen star object
is en dat centroidbeweging, rastertranslation en celidentiteit uit elkaar
moeten blijven.

De operationele tracker selecteerde op hetzelfde moment een KMI-systeem met:

- slechts twee opeenvolgende radarframes;
- gemeten intervalsnelheid 124,1 km/u;
- footprintafstand 28,3 km tot de thuistracker;
- centroidafstand 220,2 km;
- lege `ground_sources` en `ground_raining_sources`.

De footprint kan dus terecht dicht bij of over de tracker liggen terwijl het
centroid ver weg ligt. De intervalsnelheid na twee posities is wel een exacte
lineaire verplaatsing van de twee **gekozen objectposities**, maar nog geen
bewezen fysieke voortplantingssnelheid van dezelfde neerslagstructuur. Zonder
onafhankelijke identiteit en stabiele actuele polygonen kan een andere
subcel/segmentatie de tweede positie zijn.

## Datavergaring

De secundaire calibratiedatabase meldde op dit moment:

- status `ready`, schema 11;
- 195.242.752 bytes;
- 1.682 opgeslagen frames;
- 1.754 opgeslagen vergelijkingen;
- 5.849 verificatiesamples;
- 6.152 performance-/thermische samples;
- 13.185 blikseminslagen;
- 5.454 providercycli;
- 147 pending frames.

Ten opzichte van de eerder immutable gekopieerde database met 1.461 frames is
de live teller dus verder gegroeid. Dit bewijst dat er nieuwe records worden
verzameld. Aanvullende lokale code-inspectie bevestigt dat de huidige
KMI-provider na download een afgeleide `overlay` met intensity-runs maakt en
dat `_record_calibration_frame()` die overlay naar
`original_radar_frames_pending` stuurt. Het gedownloade beeld zelf wordt langs
dit operationele pad niet aan de aanwezige vNext `RawArtifactStore` geleverd.
De naam `original_radar_frames` betekent hier dus niet: exacte oorspronkelijke
upstreambytes met native grid, qualitymask, HTTP-receipt en contenthash. Een
latere read-only snapshot en schema-/artifactaudit blijven nodig.

## Operationele gezondheid

- worker online;
- queue depth 0 van capaciteit 16;
- 4.917 voltooide en 38 mislukte jobs;
- motion-, tracking-, OPERA- en radarframeprocessen healthy;
- alle 38 zichtbare failures zaten bij `virtual_probe_operational`, dat
  `ValueError` meldde en nog geen job succesvol had afgerond.

Die virtual-probefout wordt afzonderlijk lokaal onderzocht. Er wordt niets live
gewijzigd zonder expliciete toestemming.

## Waarde als toekomstig replayevent

Dit event is geschikt als kandidaat voor:

- ten minste twintig opeenvolgende native KMI- en OPERA-frames;
- brede-rain-shield- en embedded-core-detectie;
- vergelijking van centroid, contourtranslation en lokale flow;
- split/merge- en subcelcontinuïteit;
- provider-overlap en handoffanalyse;
- trackerpassage en groundmatching.

Voor een wetenschappelijke golden replay zijn nog nodig:

- exacte originele providerartifacts en receipts;
- native georeferentie, nodata- en qualitymaskers;
- immutable current-footprintpolygonen met holes/ringen;
- geblindeerde onafhankelijke cell-/lineagelabels;
- onafhankelijke wet/dry- en passage-uitkomsten;
- vooraf bevroren event- en leakagegroepen.

## Directe grondwaarneming bij passage

Omstreeks 14:39:50 CEST meldde de gebruiker vanuit de thuislocatie dat het
net was beginnen regenen. Het vastgelegde tijdstip is het onmiddellijk daarna
uitgelezen systeemuur en heeft daarom een conservatieve onzekerheid van
plus/min één minuut. Dit is onafhankelijke menselijke uitkomstinformatie:
de melding is niet afgeleid uit de radar, maar is evenmin een gekalibreerde
regenmeterwaarde.

Kort daarna preciseerde de gebruiker dat het om **heel zachte regen** ging.
Dit is als kwalitatieve intensiteit `very_light` vastgelegd. Er is bewust geen
regenintensiteit in mm/u uit afgeleid, omdat geen gekalibreerde regenmeterwaarde
werd gemeld.

Het door de gebruiker aangeduide Netatmo-station in Heffen heeft station-ID
`70:ee:50:29:59:56`. De publieke Netatmo-kaart toonde bij een directe controle
rond 14:48:52 CEST:

- plaatslabel `Mechelen` en hoogte 6 m;
- een stationsmeting van ongeveer één minuut oud;
- 0,10 mm regen in het laatste uur;
- 0,10 mm regen vandaag.

Daarmee had dit specifieke station uiterlijk bij die controle meetbare neerslag
geregistreerd. De kaartwaarde is een cumulatieve één-uurswaarde en geen
instantane `rain_live`-intensiteit. Zonder een eerdere momentopname van hetzelfde
station kan bovendien niet strikt worden bewezen dat de volledige 0,10 mm pas na
de menselijke startmelding viel; de timing is wel verenigbaar met dezelfde
zachte regenpassage.

Een hercontrole rond 14:51:59 CEST toonde dezelfde 0,10 mm en een meetleeftijd
van ongeveer vier minuten. Dat plaatst de getoonde positieve stationsmeting
grofweg rond 14:47–14:48 CEST: ongeveer acht minuten na de menselijke
startmelding, met circa twee minuten gecombineerde onzekerheid door beide
afgeronde tijdsaanduidingen. Dit is een detectiebovengrens, geen bewezen exact
beginmoment.

Tegelijk meldde STV3 nog steeds nul lokale Netatmo-stations en nul natte
Netatmo-stations. In `sensor.stv3_worker_status` stond `netatmo_fetch` gezond en
inactief, maar met nul voltooide en nul mislukte jobs sinds de workerstart. De
publieke meter was dus niet droog; de operationele STV3-inname had dit station
op dat moment niet aangeleverd.

Een derde controle rond 15:12:36 CEST versterkte dat bewijs:

- het specifieke station stond op 0,51 mm voor het laatste uur en vandaag;
- dat is 0,41 mm meer dan de eerdere 0,10 mm;
- de getoonde stationsmeting was ongeveer negen minuten oud;
- STV3 bleef `nadert_regio`, met nul lokale Netatmo-stations en nul natte
  Netatmo-stations;
- `netatmo_fetch` bleef op nul voltooide en nul mislukte jobs.

De toename bewijst dat het station tijdens deze lokale regenpassage verder
neerslag registreerde. Zij maakt de operationele discrepantie dus sterker dan
een eenmalige, mogelijk historische cumulatieve waarde.

De gelijktijdige operationele momentopname meldde:

- status `naderend` voor storm `cb17f0f9`;
- vijf opeenvolgende KMI-frames;
- contourafstand 3,7 km en contour-ETA 0 minuten;
- `passage_classification=raak` en
  `footprint_pass_distance_km=0`;
- centroidafstand 196,5 km;
- geen beschikbare intensiteitsverwachting;
- geen automatische `ground_sources`, `ground_raining_sources` of
  `ground_confirmation_basis`;
- nul lokale Netatmo-stations;
- Synoptic en METAR stonden allebei op `fout` met sterk verouderde
  sensorupdates;
- de tracker publiceerde nog steeds 114,7 km/u actueel en 109,2 km/u
  gemiddeld met `motion_basis=unknown`.

De menselijke regensstart ondersteunt dus de **lokale contourpassage**, maar
bevestigt niet de gepubliceerde object-/centroidsnelheid. Het verschil tussen
de contourafstand van 3,7 km en de centroidafstand van 196,5 km is juist
bewijs dat een brede regenzone niet als één puntobject mag worden behandeld.
Dat geen automatische grondbron de regen bevestigde, is in deze momentopname
een dekkings-/beschikbaarheidsprobleem en geen droge tegenwaarneming.

De genormaliseerde, machineleesbare observatie staat in
`.analysis/live_event_20260730_home_rain_start.json`. Zij is gemarkeerd als
kandidaat voor held-out evaluatie en mag niet als trainingslabel worden
gebruikt.

## Einde van de lokale regenepisode

Omstreeks 15:37:30 CEST meldde de gebruiker dat de regen ondertussen was
gestopt. Ook dit tijdstip is het onmiddellijk daarna uitgelezen systeemuur en
krijgt conservatief plus/min één minuut onzekerheid. Tussen de vastgelegde
start- en stopmomenten ligt ongeveer 57 minuten en 40 seconden; omdat beide
grenzen menselijke chatwaarnemingen zijn, bedraagt de conservatieve
onzekerheid op die duur plus/min twee minuten.

Deze eindmelding maakt van het event een begrensde lokale wet/dry-episode. Zij
is waardevol voor passage- en duurverificatie, maar blijft kwalitatief:

- er is geen instrumenteel bepaalde eerste of laatste druppel;
- er is geen gemeten instantane mm/u;
- een tijdelijke onderbreking vlak voor de melding kan niet worden
  uitgesloten;
- het label mag niet worden gebruikt om achteraf een modeldrempel te kiezen.

De machineleesbare eventrecord bevat daarom een afzonderlijke
`episode_end_observation`, zonder de oorspronkelijke startwaarneming te
overschrijven.
