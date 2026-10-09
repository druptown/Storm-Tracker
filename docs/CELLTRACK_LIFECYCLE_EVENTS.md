# Immutable CellTrack-lifecycle-events

## Status en afbakening

Dit document beschrijft de lokale shadowimplementatie in
`engine/cell_track_lifecycle.py`. De implementatie bestaat byte-identiek in de
Home Assistant-component en de worker-mirror, maar is bewust nog niet aan de
live runtime of het meteorologische evidence-store gekoppeld.

De lifecyclelaag registreert identiteitsovergangen nadat een caller een
detectie, assignment of expliciet einde heeft gekozen. Zij bepaalt zelf geen
associatie en bevat geen maximumsnelheid, innovation gate, afstandsafwijzing of
andere meteorologische reject. Een extreme verplaatsing blijft dus mogelijk
zodra de caller die detectie aan dezelfde track heeft toegewezen.

## Gebeurteniscontract

`CellTrackLifecycleEvent` is immutable en bevat voor elke gebeurtenis:

- `event_id`: een niet-lege, unieke ID die de caller aanlevert;
- `event_type`: `birth`, `missed`, `gap`, `reacquired`, `death`, `split` of
  `merge`;
- `valid_at`: de eindige geldigheidstijd van het frame, nooit de
  verwerkingstijd;
- `track_ids`: alle tracks waarop de gebeurtenis atomisch betrekking heeft;
- `detection_ids`: de concrete detectiereferenties waarop de beslissing rust;
- `reason`: de expliciete, niet-lege auditreden;
- `ledger_revision`: de ledgerrevision waarin de volledige gebeurtenis is
  vastgelegd;
- `track_revisions`: de revision van iedere betrokken track in diezelfde
  commit;
- `parent_track_ids` en `child_track_ids`: uitsluitend voor echte split- en
  merge-lineage;
- `algorithm_version`: `stv3-celltrack-lifecycle-event-v2`.

De ledger valideert bij constructie dat event-ID's uniek zijn, tracks en
detecties bestaan, event- en trackrevisions niet in de toekomst liggen,
detectietijden overeenstemmen met geboorte/heropname/split/merge en iedere
betrokken track terugverwijst naar het event. Een ongeldige samengestelde staat
kan daardoor niet als `CellTrackLedger` worden geconstrueerd.

De bestaande naam `CellTrackLineageEvent` blijft een alias en het bestaande
ledger-veld `lineage_events` blijft behouden. `lifecycle_events` is de nieuwe
semantische read-only alias. Op een track blijft `lineage_event_ids` het
duurzame v1-veld; `lifecycle_event_ids` geeft dezelfde tuple terug.

## Overgangen

### Birth

`record_cell_track_birth` vereist van de caller een event-ID, frame
`valid_at`, track-ID, detectie, objectsoort, beleid en reden. De detectietijd
moet exact gelijk zijn aan het frame. Track revision 1 en de birth-eventref
worden samen in één nieuwe ledgerrevision vastgelegd.

### Missed en gap

`record_cell_track_missed` en `record_cell_track_gap` registreren één
caller-benoemd frame zonder toegewezen detectie. Het event verwijst naar de
laatste werkelijk waargenomen detectie; zijn `valid_at` is de tijd van het
gemiste frame. Het frame moet strikt later liggen dan zowel de laatste detectie
als ieder eerder lifecycle-event voor die track.

Zolang `maximum_gap_frames` niet wordt overschreden, blijft de track
`coasting`. Bij overschrijding eindigt de track met
`termination_reason=gap_limit_exceeded` en wordt dezelfde atomische overgang
als `death` geregistreerd. De caller blijft eigenaar van de ingestelde
gaplimiet en de auditreden.

### Reacquired

`record_cell_track_reacquired` accepteert uitsluitend een bestaande
`coasting` track met minstens één gemist frame. De caller levert event-ID,
frametijd, detectie en reden. De detectietijd moet gelijk zijn aan de
frametijd. De detectie wordt aan dezelfde `track_id` toegevoegd, de misscounter
gaat naar nul en er worden geen parent- of childtracks aangemaakt. Een gap en
latere heropname vormen dus nadrukkelijk geen identity switch.

Er is geen afstands- of snelheidsafwijzing in deze overgang. Of een verre
detectie meteorologisch bij dezelfde track hoort, moet al door de caller en de
latere assignment-/kwaliteitslaag zijn beslist en blijft via het event
auditbaar.

### Death

`record_cell_track_death` beëindigt een actieve track op een expliciete, strikt
latere frametijd. Event-ID en reden komen van de caller. Het event verwijst naar
de laatste echte detectie; er wordt geen fictieve einddetectie of tijd
gegenereerd.

### Split en merge

De bestaande split- en mergehelpers blijven compatibel. Hun event-ID en alle
track-ID's komen van de caller; `valid_at` is exact de gemeenschappelijke
detectietijd van de nieuwe childdetectie(s). Ouders blijven als ended tracks in
de ledger aanwezig. Het event bevat alle parent-/childrefs, childdetectierefs
en de revisions uit dezelfde ledgercommit.

## Atomische assignmentmodus

De oorspronkelijke vijfargumentenaanroep van
`advance_ledger_from_assignment` blijft eventloos werken voor bestaande
shadowreplays.

Zodra één eventargument wordt meegegeven, geldt strict event mode. De caller
moet dan tegelijk leveren:

- `frame_valid_at`;
- een exacte `birth_event_ids`-mapping voor alle births;
- een exacte `missed_event_ids`-mapping voor alle unmatched vorige detecties;
- een exacte `reacquired_event_ids`-mapping voor iedere match van een
  coasting track;
- een `event_reasons`-mapping die exact alle aangeleverde event-ID's verklaart.

Alle event-ID's moeten onderling uniek en nog onbekend zijn. Alle huidige
detecties moeten exact dezelfde `frame_valid_at` hebben. Eerst wordt de
volledige context gevalideerd; pas daarna worden immutable vervangende tracks
en events gebouwd. Birth, miss/death en reacquisition uit één assignment delen
daarom één nieuwe ledgerrevision. Een validatiefout laat de inputledger
ongewijzigd.

## Backward compatibility

- Bestaande trackhelpers (`start_cell_track`, `append_cell_detection`,
  `mark_cell_track_missed`, `end_cell_track`) blijven beschikbaar.
- De vijfargumentenassignment blijft eventloos.
- Bestaande split-/merge-aanroepen blijven geldig.
- De namen `CellTrackLineageEvent`, `lineage_events` en
  `lineage_event_ids` blijven leesbaar.
- De bestaande lifecycle-algoritmeversie van tracks blijft v1; het uitgebreid
  eventcontract heeft een afzonderlijke v2-versie.

## Verplichte duurzame en live gate

Deze stap wijzigt bewust geen evidence-storeschema en wiret geen runtimecode.
Voor livegebruik moet eerst één afzonderlijke duurzame gate worden uitgevoerd:

1. definieer en migreer een schema dat alle eventvelden zonder verlies opslaat,
   inclusief ledger- en trackrevisions, detectionrefs, reden en frame
   `valid_at`;
2. leg idempotentie vast op `event_id` en definieer een hard conflict wanneer
   dezelfde ID met andere inhoud wordt aangeboden;
3. commit trackmutaties en lifecycle-event in één opslagtransactie en lever een
   duurzame receipt;
4. bewijs crashherstel en exact-once replay rond ieder commitpunt;
5. definieer retentie, export en referentiële integriteit voor events,
   detecties en tracks;
6. voeg de live caller toe die stabiele event- en track-ID's en de echte
   framegeldigheidstijd aanlevert;
7. voer release-, replay- en soaktests uit voordat de shadowcode enige
   operationele voorspelling beïnvloedt.

Tot deze gate aantoonbaar is geslaagd, zijn de events lokaal en testbaar maar
niet duurzaam en niet live actief.
