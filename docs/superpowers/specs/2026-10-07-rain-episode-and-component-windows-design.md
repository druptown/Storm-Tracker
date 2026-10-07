# Regenperiode en afzonderlijke intensiteitsvensters

Datum: 7 oktober 2026. Status: gespreksontwerp goedgekeurd; deze uitwerking
wacht op gebruikersreview. Nog geen productcode gewijzigd of gedeployed.

Dit is een aanvulling op `2026-10-01-scientific-passage-forecast-stability-design.md`.
De quantity-, motion-, lineage-, censoring- en verificatievoorwaarden daarvan
blijven gelden. De verouderde statusregel in dat eerdere document is geen
beschrijving van de huidige gedeeltelijke implementatie.

## Gewenst resultaat

Per target één doorlopende verwachte regenperiode, inclusief lichte
achtergrondregen, met daarin afzonderlijk gevolgde buiencomponenten en lokale
intensiteitsvensters. Als het tussen twee sterkere componenten blijft regenen,
blijft dat dezelfde regenperiode. Een ontbrekende track of berekening is geen
droge onderbreking. Een verdwenen eindschatting moet verklaarbaar zijn en mag
niet stil veranderen in `nog niet berekend`.

De compacte kaart toont start, vermoedelijk einde en een intensiteitsbalk.
Componentdetails zijn uitklapbaar. Voor personen blijft de bestaande
zichtbaarheidsregel buiten thuis behouden. Alle resultaten blijven voorlopig,
zonder verzonnen kansen of een claim van volledig gevalideerde nauwkeurigheid.

## Bewezen huidige tekortkomingen

- De native QPE-waarden gaan ongewijzigd naar `build_target_radar_frame`.
  `_thin_tracking_policy` detecteert rain shields vanaf 1,0 mm/u en cores
  vanaf 4,0 mm/u. `PassageForecastPolicy` gebruikt 0,1 mm/u. Die laatste
  drempel voegt geen ontbrekende pixels aan een reeds beperkt objectmasker toe.
  Een unie van bestaande celpassages reconstrueert dus niet noodzakelijk de
  lichte achtergrondregen van de volledige regenzone.
- `thin_nowcast` bewaart een aankomst alleen via het bestaande hold-pad als het
  niet regent. Bij regen zonder nieuwe passage wordt de bewaarde uitkomst
  verwijderd. De MQTT-kaart vervangt haar lijst volledig; het vorige einde
  valt hierdoor weg zonder bewijs dat de regen geëindigd is.
- Het huidige PostGIS-pad bewaart observaties, fusiesamenvattingen,
  celgeometrieën, trackpunten en immutable forecast-/passage-issues. Het
  schrijft niet het volledige fused raster met alle QPE-pixels en maskers weg.
  Een veldfingerprint is identiteit, geen herstelbare rasterinhoud.

Deze bevindingen komen uit de huidige broncode. Ze bewijzen niet dat ieder
historisch radarproduct elders wel of niet als RAW-artifact beschikbaar is.

## Gekozen aanpak en alternatieven

Gekozen: een buitenste kwantitatieve regen-envelop naast de bestaande shields
en cores, met eigen toegelaten bewegingsbewijs. Daaruit wordt de targetserie
en de regenperiode afgeleid. Bestaande componenttracks blijven bestaan voor
hun eigen passage en intensiteit. Er komt geen tweede tracking-engine.

Alleen bestaande celintervallen samenvoegen is onvoldoende door de verschillende
detectiedrempels. Eén globale gemiddelde vector op alle zichtbare echo's
toepassen is eveneens onvoldoende: regionale vectortegenstrijdigheid en
onvoldoende representatieve support zijn al in de schaduwproef aangetoond.
Een dicht variërend optical-flowveld blijft een afzonderlijke challenger,
zoals het bestaande ontwerp voorschrijft; geen verplichte pySTEPS-runtime.

## 1. Buitenste regen-envelop met eigen bewijs

Voeg aan de bestaande hiërarchische detectie een buitenste `rain_envelope`
toe op de bestaande passagegrens van 0,1 mm/u, uitsluitend voor verse,
kwantitatieve, quality-correcte QPE. De shields op 1,0 en cores op 4,0 blijven
als afzonderlijke niveaus behouden. Deze nieuwe objectsoort wordt door dezelfde
begrensde CellTrackEngine gevolgd. Kind, parentrelaties en lineage worden
expliciet opgenomen; geen automatische koppeling alleen op nabijheid.

De envelop krijgt motion-consensus op basis van zijn eigen beeld-/trackbewijs.
Een toegelaten vector van één child wordt niet aan de rest van de zone gegeven.
De bestaande bewegingskwaliteitspoorten blijven ongewijzigd. Bij onvoldoende
evidence blijft de envelop onbepaald, ook als sommige children wel voorspelbaar
zijn. De extra detectielaag mag de bestaande object-/pixel-/edge-cap niet
ongemerkt overschrijden; overflow blijft expliciet en fail-closed.

De 0,1-grens is een bestaande kandidaatparameter, geen bewezen menselijke
detectiegrens. Positieve QPE daaronder heet `below_occurrence_threshold`, niet
bewezen droog. De huidige bui wordt niet gebruikt om drempels te optimaliseren.

## 2. Targetserie, regenperiode en intensiteit

Advecteer per forecastbare envelop zijn volledige eigen QPE-masker en waarden
met zijn toegelaten vector. Anker alle toekomstige tijden aan de producttijd:
bij uniforme beweging is het bronpunt `x_target - v * (t_valid - t_radar)`.
Gebruik de bestaande forecasttijdstap van 60 seconden en maximale rekenhorizon
van twee uur. Een grotere horizon valt buiten deze wijziging.

Per tijdstap zijn quantity, mm/u, position accuracy, valid mask en bewegingssupport
bekend. De serie onderscheidt ondersteund nat, modelmatig droog, onderdrempelig
en onbekend. Nodata, niet-toegelaten beweging en ontbrekende support zijn nooit
nulregen. Een droge serie-uitkomst is een modeluitkomst, geen bewezen grondweer.

Een ondersteund regeninterval is maximaal aaneengesloten natte support.
Overlappende natte intervallen van enveloppen geven één targetregenperiode.
Een onderdrempelig of onbekend gat levert geen bewezen droogpauze op: de
continuïteit wordt expliciet onzeker en krijgt geen kunstmatig gesloten einde.
Een ondersteund modeldroog interval blijft zichtbaar als verwachte pauze; het
wordt niet weggepoetst met een willekeurige minimumduur of visuele smoothing.

Entry/exit blijven brackets. Reeds nat bij uitgifte betekent left-censored:
uitgiftetijd is niet het oorspronkelijke begin. Wet support tot aan de horizon
geeft right-censored en `einde buiten berekeningsvenster`, niet een eindtijd
gelijk aan de horizon. Een onbekende tussenperiode krijgt een eigen reden,
niet een precies ogende totale duur.

De hoofdintensiteitsserie komt uit lokaal gesamplede geadveceerde envelop-QPE,
niet uit het systeemmaximum. Iedere component behoudt daarnaast eigen
track-ID, motion, passagebrackets en lokale samples. Parent-QPE en child-QPE
zijn geen optelbare onafhankelijke regenbijdragen. Gebruik de parentserie voor
het hoofdvenster en de intensiteitsbalk; componentvensters zijn traceerbare
detailprognoses. Als hun eigen beweging een afwijkende timing voorspelt, toon
die afwijking als onzekerheid; vervang niet ongemerkt de hoofdserie.

Bij meerdere overlappende envelopprojecties wordt regenoccurrence als unie
genomen. Een eenduidige lokale QPE-waarde mag alleen bij dezelfde bronpixel/
supportidentiteit of eenduidige toewijzing worden gebruikt. Conflicterende
waarden blijven alternatieven of `intensity_ambiguous`, nooit een som of
willekeurig gekozen piek. Een onbekende intensiteit is geen droge episode.

## 3. Continuïteit en het vorige einde

Episode-state is per targetpositie, accuracy, regio-revisie en lineagealias.
De bestaande registry blijft de enige targetcontinuïteitsautoriteit; geen
tweede globale assignment. Alleen nieuwe unieke geschikte radarproducttijden
tellen als nieuwe support. Bewaar origineel issue-ID, radarvalidtijd en tijden.

Ontbreekt nieuwe support terwijl het target nat is, publiceer de vorige
eindschatting apart als `previous_end_estimate`, met oorspronkelijke uitgifte,
ouderdom en status `coasting`. Het huidige berekende einde blijft dan null.
De UI zegt bijvoorbeeld `vorige schatting 20:54 — onzeker`, niet dat deze
opnieuw berekend is. Hanteer maximaal de bestaande 900 seconden vanaf de
laatste echte support en nooit voorbij de oorspronkelijke horizon. Een tick
verjongt deze termijn niet. Een schatting in het verleden blijft geschiedenis,
niet een nieuw toekomstig einde.

Na verlopen support wordt het actuele einde `onvoldoende betrouwbare data`;
de oorspronkelijke schatting blijft alleen als historische informatie zichtbaar.
Verse tegenstrijdige support vervangt of weerlegt de oude schatting met reden.
Het bereiken van een voorspelde eindtijd beëindigt de episode niet. Werkelijk
radareinde krijgt een laatste-natte/eerste-droge bracket op unieke geschikte
scans volgens de bestaande statepolicy. Nabije stations zijn afzonderlijke
puntmetingen, niet automatisch exact-targetwaarheid.

Een targetverplaatsing of incompatibele regio-/grid-/quantitywissel maakt
oude operationele support onbruikbaar. Ongewijzigde targets in dezelfde regio
verliezen hierdoor hun eigen state niet. Herstart geeft eerst recovering;
oude issues worden niet nieuw actueel verklaard.

## 4. Publicatie en opslag

Behoud het bestaande passageschema voor componentissues. Voeg een afzonderlijk
versioned episodebericht toe onder `storm_tracker/v1/operational/episodes`,
schema 1, met issue-ID, targetpositie/revisie, episodealias, tijden/brackets,
censoring, continuïteitsstatus, volledige/partiële support, redenencodes,
originele vorige eindschatting en component-issueverwijzingen.
De bestaande exacte snapshotkeys worden niet stil uitgebreid.

Bewaar episode-output append-only in versioned forecast_metadata van de
bestaande forecast-issueopslag, met eigen algorithmversion in de issue-ID.
Component-issues blijven afzonderlijk immutable. Gebruik bestaande PostGIS-
tabellen; geen nieuwe infrastructuur, databasewisactie of workerstartaudit.

Volledige rasterarchivering is een afzonderlijke opslagbeslissing. Deze
wijziging claimt geen volledige pixelreplay zolang herstelbare QPE-arrays,
validiteitsmaskers, grid, quantity, bronnen en tijdrevisies niet aantoonbaar
bewaard zijn. Breid die opslag niet zonder expliciete toestemming uit.

De gebruiker wil deze volledige passage als reproduceerbare developmentcase
bewaren. Een replaywaardige vastlegging vereist opeenvolgende oorspronkelijke
radarvelden, product- én ontvangsttijden, grid/maskers/units, targetposities en
revisies, beschikbare puntmetingen, code/policyversies en de oorspronkelijke
uitgegeven voorspellingen. Reconstructie moet causale beschikbaarheid volgen:
een later ontvangen product mag niet in een eerdere simulatiestap worden
gebruikt. Ontbrekende velden blijven expliciete gaten; contouren of screenshots
zijn geen vervanging. De volledige beschikbaarheid is nog niet geverifieerd.
Afstellen op deze passage maakt haar een developmentcase; de uiteindelijke
aanvaardbare voorspellingsrange moet op andere ongebruikte passages worden
beoordeeld. Radar blijft een referentiemeting met eigen onzekerheid, niet exact
grondweer op elke positie.

Begrens publicatie op 32 episodes per target en 121 samples per episode;
metadata maximaal 64 KiB per issue. Rapporteer truncatie expliciet en noem
een incompleet bericht nooit een volledige voorspelling. Deel regionale
verwerking tussen targets, niet hun positiegebonden state. PostGIS-uitval
blokkeert de actuele MQTT-publicatie niet.

## 5. Gerichte acceptatie en live verificatie

Test vóór productcode minimaal:

- Twee sterkere cellen met 0,2 mm/u achtergrond ertussen blijven één episode.
- Een 0,2-mm/u envelop zonder >=1-mm/u shield heeft eigen motion-admission
  nodig; een gebrek daaraan levert geen verzonnen voorspelling op.
- Een ondersteunde modeldroge pauze blijft apart; nodata en onderdrempelige
  waarden geven geen false-dry of verzonnen finite einde.
- Parent/child en overlappende projecties tellen QPE niet dubbel.
- Dezelfde radarvalidtijd verjongt geen coasting of bevestigingsteller.
- Ontbrekende tracking tijdens regen bewaart de oude issue als onzeker;
  expiry, voorbijgaande eindtijd, incompatibele positie en restart blijven eerlijk.
- Colocated targets krijgen gelijkwaardige meteorologische resultaten;
  targetverplaatsing verandert niet de state van een ander target.
- Bronpixeloffsets, radarvalidtijd-anker, twee-uurscensoring en schema-
  compatibiliteit blijven behouden. Extra objecten overschrijden geen cap stil.
- Dezelfde episode/componentissues komen met dezelfde tijden en units in
  MQTT, opslag en dashboard; geen historische uitkomst wordt overschreven.

Daarna runbookdeploy uitsluitend naar NAS-HA en prospectieve verificatie op
normale broncadans. Bewaar alle uitgiften vóór beoordeling, inclusief missende
waarschuwingen. Vergelijk begin/einde en intensiteitsvensters met latere
radarbrackets en passende onafhankelijke puntwaarnemingen. De passage van
7 oktober is een development/regressiecase, geen held-out kalibratiebewijs.
Geen brede historische suite, gedwongen polling, persoonlijke verplaatsing,
Netatmo/importerwijziging of Core/VM-herstart.

## Wetenschappelijke onderbouwing en beperkingen

Veldadvectie gebruikt een neerslagveld plus beweging om toekomstige lokale
waarden te berekenen. Dit ondersteunt het scheiden van het totale
regenverloop en objectidentiteit; het bewijst deze specifieke implementatie
niet. Zie [pySTEPS extrapolatie](https://pysteps.readthedocs.io/en/stable/generated/pysteps.nowcasts.extrapolation.forecast.html).

Deterministische advectie voorspelt groei/verval en nieuwe initiatie niet
volledig. De tijd-/schaalafhankelijke evolutie uit
[Pulkkinen et al. (2019)](https://gmd.copernicus.org/articles/12/4185/2019/)
onderstreept deze beperking. Hier worden geen ensemblekansen, kalibratie,
MCS-classificatie of gegarandeerde skillhorizon toegevoegd.
