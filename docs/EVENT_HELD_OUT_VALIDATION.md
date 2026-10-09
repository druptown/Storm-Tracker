# Event-held-out wetenschappelijke validatie

Status: lokaal, pure shadow-/validatiecode; niet live gekoppeld en niet
gedeployed.

## Doel

De validatielaag voorkomt dat interne engine-output, gecorreleerde frames of
achteraf gekozen vrijgavegrenzen als onafhankelijke meteorologische validatie
worden gepresenteerd. De laag maakt geen labels en kiest geen productiedrempels.
Ontbrekende informatie blijft `INDETERMINATE`.

De implementatie bestaat uit:

- `storm_tracker_worker/validation_corpus.py`: event/group-split,
  bronprovenance en getypeerde externe labels;
- `storm_tracker_worker/scientific_validation.py`: preregistratie,
  praktische marges en fail-closed releasegates;
- `tests/test_validation_corpus.py`;
- `tests/test_heldout_scientific_validation.py`;
- `tests/test_scientific_validation.py`.

## 1. Leakage-safe eventsplit

`EventFrame` vereist per frame:

- een onveranderlijk `frame_id`;
- één `event_id` voor het meteorologische event;
- de meteorologische geldigheidstijd;
- alle bekende `leakage_group_ids`.

Een leakagegroep verbindt events die niet onafhankelijk mogen worden
geëvalueerd, bijvoorbeeld dezelfde fysieke storm over een providerhandoff,
radaroverlap of annotatiebatch. De verbinding is transitief: als A met B en B
met C is verbonden, worden A, B en C één component.

`build_event_held_out_split()`:

1. sorteert input onafhankelijk van aanlevervolgorde;
2. vormt connected components van events en leakagegroepen;
3. hasht de volledige eventcomponent met SHA-256;
4. wijst de hele component via een vaste bucket toe;
5. controleert dat frame, event en leakagegroep nooit partitions kruisen;
6. levert hashes van corpus, policy en splitresultaat.

De splitter kent bewust geen standaardverdeling. Een
`EventSplitPolicy` moet vooraf expliciet train/validation/test-basispunten,
een salt, policy-ID en algoritmeversie vastleggen. De drie positieve gewichten
moeten exact 10.000 basispunten vormen.

Belangrijke grens: software kan niet zelfstandig weten dat twee verschillend
genoemde events fysiek dezelfde storm zijn. De eventcatalogus en alle
leakagegroepen moeten daarom extern en vóór de split worden vastgesteld. Een
ontbrekende koppeling kan nog steeds meteorologische leakage veroorzaken.

## 2. Externe labelcontracten

Elke `ExternalLabelSource` legt vast:

- source-ID en sourcetype;
- producent, datasetversie en methodeversie;
- SHA-256 van het exacte bronartifact;
- onafhankelijkheid van de kandidaatengine;
- blindering voor kandidaatoutput.

Engine-afgeleide diagnostiek is nooit externe waarheid. Menselijke consensus
is alleen eligible als die blind voor kandidaatoutput is. In-situ instrumenten
en onafhankelijke referentieanalyses moeten expliciet kandidaat-onafhankelijk
zijn.

Labels hebben een gemeenschappelijke envelope met label-ID, source-ID,
event-ID, frame-ID en toestand:

- `confirmed`: bruikbaar als alle andere contracten slagen;
- `ambiguous`: bewaard, maar niet bruikbaar voor een releasegate;
- `unknown`: bewaard, maar niet bruikbaar voor een releasegate.

De payload is strikt getypeerd:

| Labeltype | Verplichte betekenis |
|---|---|
| `cell_identity` | referentiecel en optionele parentreferenties voor lineage |
| `cell_position` | latitude/longitude en expliciete of onbekende onzekerheid |
| `cell_speed` | oost/noord-vector, echt tijdsinterval en onzekerheid |
| `passage` | tracker, evaluatievenster, occurrence en consistente passagetijd |
| `wet_dry` | fysieke site, meetinterval, wet/dry/unknown en dry-capability |
| `precipitation_field` | immutable veldartifact, grid, variabele, eenheid en tijd |

Een `dry`-label zonder aantoonbaar dry-capable instrument wordt geweigerd.
Een bevestigde passage zonder passagetijd wordt geweigerd. Payloadtypes kunnen
niet onderling worden verwisseld.

`ExternalTruthContext` koppelt labels aan exact één immutable eventsplit en
weigert onbekende frames/events, verkeerde frame-eventkoppelingen en
partitionoverschrijding.

## 3. Preregistratiemanifest

Een `ValidationManifest` bevat naast gates:

- immutable `PreregistrationSource`-records met rol `external_label`,
  `baseline` of `margin_rationale`;
- per externe gate een `GateEvidenceContract`;
- per gate die een praktische beslisgrens nodig heeft een
  `PracticalMargin`.

Een extern evidencecontract legt vooraf vast:

- vereiste labeltypes;
- toegelaten labelbronnen;
- toegelaten baselinebronnen;
- minimumaantal onafhankelijke labelbronnen;
- de enige toegelaten partition, normaal `test`.

Een praktische marge heeft één expliciete interpretatie:

- `absolute_threshold`;
- `minimum_improvement`;
- `noninferiority_tolerance`.

Een marge is pas resolved als waarde, eenheid, rationale en immutable
rationalebronnen aanwezig zijn. Voor een absolute marge moet de waarde exact
gelijk zijn aan de gate-threshold. De comparator en margemodus moeten
semantisch bij elkaar passen.

`lock()` weigert onder andere:

- een `UNSET` corpus of splitpolicy;
- een onopgeloste vereiste threshold;
- een ontbrekende waarde, eenheid, rationale of rationalebron;
- een ontbrekend extern evidencecontract;
- ontbrekende externe label- of baselinebronnen;
- een bron met verkeerde rol;
- een label- of baselinebron die niet kandidaat-onafhankelijk is.

De roadmapmanifesten laten de empirisch nog niet onderbouwde waarden, eenheden,
rationales en echte bronnen doelbewust `None`/leeg. Ze zijn dus deterministisch
hashbaar, maar niet lockable.

## 4. Fail-closed evaluatie

Voor een gate met `requires_external_truth=True` is een boolean
`external_truth_available=True` niet voldoende. `evaluate_manifest()` vereist:

1. een reeds gelockt manifest;
2. exact hetzelfde corpus-ID en dezelfde splitpolicyhash;
3. uitsluitend events uit de preregistreerde partition;
4. `event_count` gelijk aan het aantal unieke event-IDs;
5. confirmed labels van het juiste type voor ieder geëvalueerd event;
6. alleen vooraf toegelaten, onafhankelijke sources;
7. een exacte match van source-artifact-SHA-256;
8. metricberekeningsprovenance;
9. vooraf toegelaten baselinebronprovenance waar vereist;
10. voldoende events en onafhankelijke labelbronnen.

Iedere ontbrekende of afwijkende schakel levert `INDETERMINATE`; niet `PASS`.
Een unlocked manifest toont geen held-out gate-uitkomst. Daarmee kan een
testresultaat niet via deze evaluator worden bekeken en daarna nog worden
gebruikt om het manifest te locken.

De gates evalueren releases. Ze verwijderen nooit een echte radarobservatie,
cel, track of groundmeting.

## 5. Wat nog echte data en menselijke keuzes vereist

De code levert nog geen meteorologische skillclaim. Voor lock en evaluatie
ontbreken bewust:

- een door meteorologen gecontroleerde eventcatalogus;
- alle fysieke-storm/leakagegroeplinks;
- expliciet gekozen splitgewichten en salt;
- onafhankelijke, geblindeerde cell-/lineagelabels;
- onafhankelijke positie- en snelheidsreferenties met onzekerheid;
- trackerpassage-outcomes;
- wet/dry-referenties met bewezen sensorcapability;
- onafhankelijke neerslagvelden voor FSS;
- immutable baseline-implementaties en hun hashes;
- empirisch en operationeel onderbouwde praktische marges, eenheden en
  rationales;
- vooraf vastgelegde minimum-eventsteun en relevante strata;
- lock van manifesthash vóór iemand testuitkomsten bekijkt.

De numerieke waarden in unittests zijn uitsluitend synthetische fixtures om
vergelijkingsrichting en fail-closed gedrag te testen. Ze worden nergens als
productiedrempel of echt label geëxporteerd.
