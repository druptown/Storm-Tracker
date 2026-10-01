# Wetenschappelijk stabiele buipassagevoorspelling

**Datum:** 1 oktober 2026

**Status:** geschreven ontwerp ter beoordeling, nog niet geïmplementeerd

**Bouwt voort op:**
`2026-09-23-footprint-ground-validation-replay-design.md`,
`VNEXT_NOWCAST_BASELINES_AND_FORECAST_CONTRACT.md` en
`VNEXT_FORECAST_VERIFICATION.md`

## Doel

Storm Tracker moet een naderende buienzone over opeenvolgende unieke
radarbeelden als hetzelfde meteorologische systeem blijven volgen. Het systeem
moet aankomst, einde en lokale intensiteit berekenen uit de bewegende
regenvoetafdruk en de beschikbare kwantitatieve radars, met een expliciete
onzekerheidsmarge wanneer de meetreeks nog onvoldoende informatie bevat.

Een dashboardstatus mag nooit betrouwbaarder ogen dan de meteorologische
evidence. Een ontbrekende of afwijkende scan wordt daarom niet als droogte of
`geen_dreiging` geïnterpreteerd. De verbetering blijft onderdeel van het dunne
operationele pad en introduceert geen nieuwe provider-, audit-, authority- of
opstartinfrastructuur.

## Aanleiding en vastgestelde fouten

De live passage bij Koblenz van 30 september op 1 oktober 2026 leverde het
volgende controlepunt:

- eerste voorspelling om 22:37 voor aankomst om 23:57;
- latere radardetectie op de targetlocatie om 00:02, dus circa vijf minuten
  aankomstfout op de eerste voorspelling;
- eerste voorspelde duur circa vijf minuten tegenover circa 41 minuten
  aaneengesloten radarecho;
- 72 snapshots met `regen_op_komst` en 55 tussentijdse snapshots met
  `geen_dreiging` vóór de passage;
- twee WeatherSystem-ID's voor de gevolgde buienzone;
- voorspelde lokale intensiteit van ongeveer 1,2--1,5 mm/u tegenover een
  latere eerste-bandpiek van ongeveer 1,0 mm/u op de fused radarpositie;
- geen onafhankelijk grondstation met regenmeting binnen 50 km.

De test bewijst dus een bruikbaar bewegingssignaal, maar ook instabiele
identiteit, te smalle passagevensters en onvoldoende lokale
intensiteitssemantiek. Dit ene geval is een regressiecase, geen dataset om
parameters meteorologisch op te kalibreren.

## Ontwerpprincipes

1. Alleen nieuwe radar-valid-times tellen als nieuwe evidence. Herhaalde
   workercycli met hetzelfde radarbeeld mogen geen bevestiging of afwijzing
   toevoegen.
2. Afwezig bewijs is geen bewijs van afwezigheid. Een ontbrekende,
   onvolledige, stale of onbruikbare scan levert `indeterminate` of `coasting`,
   nooit automatisch `geen_dreiging`.
3. Cel- of systeemidentiteit en waarschuwingstatus zijn verschillende zaken.
   Een technische ID-wissel mag een meteorologisch ondersteunde passage niet
   wissen.
4. De volledige footprint en het kwantitatieve raster zijn leidend. Een
   middelpuntlijn met vaste straal blijft alleen een expliciet gemarkeerde
   fallback zonder nauwkeurige eindtijd.
5. Een puntwaarde wordt alleen getoond wanneer de evidence die precisie
   ondersteunt. Anders worden een bereik en concrete onzekerheidsredenen
   gepubliceerd.
6. Live verwerking blijft begrensd. Replay en kalibratie draaien uitsluitend
   offline en blokkeren geen workerstart.

## 1. Duurzame meteorologische systeemidentiteit

Boven de bestaande CellTracks en WeatherSystem-groepering komt één dunne,
stateful associatielaag voor forecastsystemen. Zij verwerkt uitsluitend nieuwe
radarframes en koppelt een actueel systeem aan eerder actieve systemen met de
volgende evidence:

- overlap tussen de actuele footprint en de met de vorige beweging
  geadveceerde footprint;
- afstand tussen voorspeld en actueel zwaartepunt, rekening houdend met het
  echte tijdsverschil;
- overeenkomst van bewegingsrichting en snelheid inclusief bestaande
  motion-onzekerheid;
- overeenkomst van footprintoppervlak en kwantitatieve intensiteitsverdeling;
- bestaande CellTrack-parent-, child- en lineage-relaties bij splitsen en
  samensmelten.

De associatie gebruikt de bestaande globale assignment- en
CellTrack-lifecycleconcepten. Er komt geen tweede algemene tracking-engine.
Ieder forecastsysteem krijgt een stabiele `forecast_system_id` en bewaart de
onderliggende CellTrack- en WeatherSystem-ID's als provenance.

Bij een splitsing behoudt de best ondersteunde voortzetting de primaire
identiteit; andere delen krijgen een afgeleide identiteit met een expliciete
parentrelatie. Bij een samensmelting blijft de best ondersteunde actieve
identiteit primair en worden de overige identiteiten als merged lineage
vastgelegd. Wanneer de associatie niet eenduidig is, blijft de identiteit
`ambiguous`; zij wordt niet stil aan de dichtstbijzijnde cel toegewezen.

## 2. Motion uit meerdere unieke beelden

Een passage mag na twee unieke frames een voorlopige aankomst geven wanneer
de bestaande motion-consensus `ready_medium` of `ready_high` is. Een
determinate eindtijd vereist minimaal drie unieke frames.

De bewegingsschatting gebruikt maximaal de laatste zes unieke beelden binnen
30 minuten. De bestaande recursive estimator, Lucas--Kanade-uitkomst en
footprintverplaatsing leveren motionkandidaten met hun eigen provenance.
Kandidaten die volgens de bestaande consensusregels onbruikbaar zijn, worden
niet in een gemiddelde verborgen.

Voor de toegelaten kandidaten worden robuuste centrale oost- en
noordcomponenten bepaald. Vanaf drie kandidaten wordt eerst de
componentgewijze mediane vector bepaald. Van iedere kandidaat wordt daarna de
Euclidische vectorafstand tot die mediaan berekend. Een afstand groter dan de
mediane afstand plus drie maal `1.4826 * MAD` wordt als outlier gemarkeerd.
Wanneer de MAD nul is, wordt geen kandidaat uitsluitend op deze regel
verwijderd en blijft eventuele ambiguïte expliciet zichtbaar. De ruwe waarden
en afwijzingsreden blijven altijd bewaard. De uitkomst is een deterministische
kinematische scenarioset, geen ongekalibreerde kansverdeling.

## 3. Footprintpassage en onzekerheidsvensters

Voor ieder toegelaten motionscenario worden de recentste footprints naar alle
toekomstige valid-times binnen de bestaande horizon geadveceerd. De
targetpositie wordt tegen iedere volledige geprojecteerde footprint getest.

De forecast bevat:

- centraal aankomst- en eindmoment als mediaan van de toegelaten scenario's;
- `entry_earliest`, `entry_latest`, `exit_earliest` en `exit_latest` als het
  bereik van de toegelaten, niet-outlier scenario's;
- gebruikte frame-, track-, systeem- en field-ID's;
- aantal unieke supportframes en werkelijk bestreken tijd;
- motionstatus, fallbackstatus en concrete onzekerheidsredenen;
- expliciete markering wanneer de forecast aan de twee-uurshorizon eindigt.

De bereiken heten `kinematic_range` en krijgen geen probabiliteitspercentage.
Een kans of formeel quantiel mag pas via het bestaande `TrackerForecast`-
contract worden gepubliceerd wanneer er een geldige ensemble-frequency of
empirische kalibratiebasis bestaat.

Met slechts twee unieke beelden mag de UI een voorlopige aankomst tonen, maar
geen exact einde. Met drie of meer geldige beelden mag een centraal einde plus
kinematisch bereik worden getoond. Een centerline-fallback mag nooit een
nauwkeurige duur claimen.

## 4. Evidencegestuurde waarschuwingsstatus

Per target wordt een passagecandidate bewaard op basis van
`forecast_system_id`, niet op basis van de wisselende actuele cel-ID.

De statusmachine gebruikt uitsluitend unieke frames:

- `candidate`: één geldige passageprojectie;
- `confirmed`: twee opeenvolgende unieke frames ondersteunen de passage;
- `coasting`: een bevestigde passage mist tijdelijk geldige nieuwe
  projectie-evidence, terwijl geen geldige scan de passage heeft weerlegd;
- `current`: verse radar- of geldige lokale grondevidence toont neerslag op
  het target;
- `ended`: verse evidence toont dat de laatste passagefootprint voorbij is;
- `cancelled`: twee opeenvolgende volledige, geldige unieke frames tonen dat
  geen lineageverwante footprint het target binnen het relevante
  kinematische venster zal raken.

`coasting` bewaart de laatst geldige tijden en maakt de onzekerheid groter. Het
is geen vaste visuele timer. Stale of herhaalde frames tellen niet mee voor
bevestiging of annulering. Een andere motion-ready cel die het target mist,
mag een bestaande bevestigde passage niet wissen.

Wanneer meerdere systemen het target kunnen raken, blijven de passages
afzonderlijk bestaan. De operationele samenvatting kiest de eerstvolgende
bevestigde passage; dashboard en PostGIS behouden de overige kandidaten in
chronologische volgorde.

## 5. Lokale intensiteit

De intensiteit van een volledig WeatherSystem is geen lokale targetintensiteit.
Voor iedere toekomstige timestep wordt daarom het geadveceerde kwantitatieve
regenraster op de targetpositie bemonsterd met de bestaande grid- en
nodatasemantiek.

Per passage worden minimaal bewaard:

- verwachte lokale gemiddelde intensiteit tijdens de passage;
- verwachte lokale piekintensiteit;
- minimum en maximum over de toegelaten kinematische scenario's;
- bronvelden, eenheid, valid-times en nodatastatus.

Wanneer alleen een footprint zonder betrouwbaar kwantitatief raster bestaat,
blijft de passage bruikbaar maar is intensiteit `unknown`. Het systeemgemiddelde
mag als expliciet gelabelde fallback worden getoond, nooit als lokaal gemeten
of lokaal voorspeld getal.

## 6. Opslag en compatibiliteit

De bestaande `stv3_target_passage`, `stv3_target_forecast`,
`stv3_storm_cell`, `stv3_track_point` en fusiongegevens blijven leidend.
Forecastsystem-ID, lineage, supportframes, kinematische bereiken,
statusovergangen en lokale intensiteitssamenvatting worden als versioned JSON
opgeslagen. Passagegebonden evidence komt in de bestaande
`stv3_target_passage.metadata`. De bestaande `stv3_target_forecast` krijgt
idempotent één `forecast_metadata jsonb`-kolom, zodat ook `candidate`,
`coasting`, `cancelled` en hun evidence per snapshot bewaard blijven wanneer
er op dat moment geen passagegeometrie bestaat. Er komt geen nieuwe algemene
infrastructuurtabel.

De bestaande MQTT-velden blijven beschikbaar. Nieuwe velden zijn additief.
Het dashboard toont `voorlopig`, `bevestigd`, `onzeker/coasting`, `actueel` of
`afgelopen`, plus het tijdsbereik. Een exacte oude cel-ID blijft diagnostische
provenance en wordt niet als gebruikersbegrip gepresenteerd.

Een algoritmewijziging krijgt een nieuwe versie. Oorspronkelijke voorspellingen
worden nooit achteraf overschreven door latere werkelijkheid.

## 7. Verificatie en kalibratie

De Koblenz-passage wordt een vaste regressiereplay met de bewaarde
issue-time-voorspellingen en latere radarframes. Latere radar is geldige
temporale verificatie van radar-nowcasting, maar wordt niet als onafhankelijke
grondwaarheid beschreven. Een nabij grondstation met regenmeting blijft de
sterkere onafhankelijke validatie wanneer dat beschikbaar is.

Offline replay rapporteert per onafhankelijk meteorologisch event:

- waarschuwingstijd en continuïteit;
- hit, miss en vals alarm;
- absolute en signed fout op entry en exit;
- fout op passage­duur;
- dekking van de gepubliceerde kinematische entry- en exitranges;
- fout op lokale gemiddelde en piekintensiteit;
- aantal identity switches, coastingframes, splits en merges.

De bestaande forecast-verificatie blijft leidend voor latere formele
probabilistische scores. Er worden geen providergewichten, confidencewaarden
of drempels op basis van uitsluitend de Koblenz-case aangepast.

## Foutafhandeling

- Minder dan twee unieke bruikbare frames geeft geen bewegingsvoorspelling.
- Twee frames mogen een voorlopige aankomst geven maar geen exact einde.
- Minder dan drie toegelaten scenario's geeft geen kinematisch bereik.
- Een analysis-grid-wijziging sluit oude en nieuwe rasterpixels niet stil op
  elkaar aan; de bestaande gecontroleerde trackerreset blijft van kracht.
- Nodata en buiten-grid worden nooit als nulregen geïnterpreteerd.
- Een PostGIS-storing verhindert de actuele MQTT-publicatie niet.
- Replay, verificatie en kalibratie draaien nooit tijdens workerstart.

## Acceptatiecriteria

1. Eén ontbrekend of onbruikbaar uniek radarframe kan een bevestigde passage
   niet naar `geen_dreiging` wijzigen.
2. Twee opeenvolgende volledige unieke frames die de passage geldig weerleggen,
   beëindigen de waarschuwing aantoonbaar.
3. Herhaalde workercycli met dezelfde radar-valid-time veranderen geen
   bevestigings-, coasting- of annuleringsteller.
4. Een split/merge-sequentie behoudt auditable lineage en veroorzaakt geen
   onverklaarde targetwaarschuwingsreset.
5. Meerdere naderende systemen blijven afzonderlijke, chronologisch
   gesorteerde passages.
6. Met twee supportframes wordt geen exact passage-einde gepubliceerd; vanaf
   drie geldige frames worden centrale tijden en een kinematisch bereik
   gepubliceerd.
7. Lokale intensiteit komt uit targetbemonstering van het geadveceerde
   kwantitatieve raster; system-wide waarden zijn uitsluitend expliciete
   fallback.
8. De Koblenz-regressiereplay behoudt de vroege hit, verwijdert de
   ongerechtvaardigde `geen_dreiging`-wissels en rapporteert de entry-, exit-,
   duur- en intensiteitsfouten zonder de case als kalibratiedataset te gebruiken.
9. Synthetische tests dekken rechte beweging, versnelling, vormgroei,
   vormkrimp, dropout, split, merge, twee passages en een echte miss.
10. Bestaande providerophaling, workerstart, PostGIS-retentie en huidige
    radarweergave krijgen geen nieuwe audit- of opstartafhankelijkheid.

## Uitgesloten uit deze wijziging

- automatisch herwegen van providers op basis van één of enkele events;
- een nieuwe stationimporter of grondstationcatalogus;
- pySTEPS als verplichte runtime;
- meldingen naar de NUC-HA;
- nieuwe RAW-, manifest-, authority-, ledger- of opstartaudits;
- cosmetische waarschuwingstimers zonder meteorologische evidence.
