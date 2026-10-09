# Multi-source operational cycle

## Doel

`storm_tracker_worker.multi_source_operational_cycle` is de gesloten,
worker-owned invoergrens voor multi-source verwerking. Een cycle bevat voor
iedere catalogusbron exact één van deze statussen:

- `current`: er is een verse, inhoudsgeadresseerde bronbijdrage;
- `stale`: er is brondata, maar de freshness-deadline is verstreken;
- `error`: de toepasselijke bron kon deze cycle niet worden verwerkt;
- `blocked`: contract, authority, credential of uitvoering blokkeert de bron;
- `not_applicable`: geen enkel target ligt binnen de expliciete catalogusscope.

`unknown` bestaat niet in dit contract. Een ontbrekende bronrij, targetrij,
reden, databijdrage of hash maakt de volledige cycle ongeldig. Dat voorkomt dat
een dashboard een stille ontbrekende bron voor nul neerslag, geen bliksem of
geen dreiging aanziet.

Deze laag doet geen netwerkverkeer, bestands- of database-I/O, decodering,
meteorologische fusie, admission, voorspelling of waarschuwing. `current`
betekent hier uitsluitend dat een exact broninputartefact volgens de
opgegeven freshness geldig is. Het is geen operationele weerclaim.

## De correcte catalogustelling

De actuele acquisition catalog bevat 27 cadence-identities. Daarvan zijn er
26 dispatchbaar en is `radar.kmi.radqpe_native` als cadence-capability extern
geblokkeerd. Daarnaast bevat de catalog één push-ingress voor Blitzortung.

De cycle bevat daarom exact 28 bronrijen:

| Catalogusdeel | Aantal | Cyclebehandeling |
| --- | ---: | --- |
| Dispatchbare cadence-workloads | 26 | data of expliciete runtime-status |
| Extern geblokkeerde cadence-capability | 1 | `blocked` wanneer toepasselijk |
| Blitzortung event-ingress | 1 | `blocked` zolang ownership ontbreekt |
| Totaal | 28 | exact 28, nooit 27 of minder |

De oudere telling “27 bronnen” is dus de dispatchbare handler-/workloadset,
niet de volledige broncatalogus. De geblokkeerde KMI-native capability
weglaten zou precies de silent-omissionfout introduceren die dit contract
moet voorkomen.

## Gesloten bronmatrix

Country-allowlists zijn leidend als een target een landcode heeft. Alleen bij
een onbekende target-landcode mag een bbox-match als fallback dienen. Een
bron zonder country-allowlist gebruikt zijn bbox. Een bron zonder beide is
globaal. Beide beoordelingen blijven per target in de bijdrage zichtbaar.

| Source ID | Soort | Catalogusdomain of vaste toestand |
| --- | --- | --- |
| `radar.aemet.composite` | radar | ES; bbox 25/-20/47/8 |
| `radar.dpc.sri` | radar | IT; bbox 34/5/48.5/20 |
| `radar.dwd.radolan_rv` | radar | AT, BE, CH, CZ, DE, DK, FR, LU, NL, PL; bbox 45/-5/58/20 |
| `radar.kmi.appv4_visual` | radar | BE, LU; bbox 46.5/-2.5/53/10.5 |
| `radar.kmi.public_wcs_unknown_quantity` | radar | BE, LU; bbox 48.5/2/52.5/7 |
| `radar.kmi.radqpe_native` | radar | BE, LU; extern contract geblokkeerd |
| `radar.knmi.rtcor` | radar | BE, DE, NL; bbox 49/1/55.5/9 |
| `radar.meteofrance.dpradar` | radar | FR; bbox 40/-7/53/11 |
| `radar.metoffice.uk_rate` | radar | GB, IE; bbox 43/-19/64/10 |
| `radar.opera.dbzh` | radar | bbox 30/-42/69/60 |
| `radar.rainviewer.visual` | radar | globaal |
| `satellite.goes18.rrqpe` | satellite | bbox -65/-180/65/-55 |
| `satellite.goes19.rrqpe` | satellite | bbox -65/-140/65/20 |
| `satellite.hsaf.h40b` | satellite | bbox -70/-70/70/70 |
| `satellite.noaa.err` | satellite | globaal |
| `lightning.goes18.glm` | lightning | bbox -65/-180/65/-55 |
| `lightning.goes19.glm` | lightning | bbox -65/-140/65/20 |
| `lightning.eumetsat.li` | lightning | bbox -70/-70/70/70 |
| `lightning.blitzortung.event_ingress` | lightning | globaal; ownership geblokkeerd |
| `ground.kmi.aws_10min` | ground | BE; bbox 49.4/2.4/51.6/6.5 |
| `ground.metar.global` | ground | globaal |
| `ground.netatmo.region` | ground | globaal |
| `ground.synoptic.region` | ground | bbox 10/-180/75/-45 |
| `model.openmeteo.italiameteo_icon2i.region` | model | IT; bbox 35/5.5/48.5/19 |
| `model.openmeteo.region` | model | globaal |
| `validation.geosphere.location` | validation | AT; bbox 45.5/8.1/49.5/17.75 |
| `validation.italiameteo.dpc_sri_historical_catalog` | validation | IT; bbox 34/5/48.5/20 |
| `validation.meteolux.location` | validation | LU; bbox 48.5/4.5/51/7.5 |

De bronsoort `validation` is in deze grens bewust afgesplitst van de upstream
plannerklasse `model`. Daardoor kan fusie guidance en verificatie niet per
ongeluk als dezelfde soort meteorologische input behandelen.

## Inhoudsgeadresseerde bijdragen

Iedere `SourceContribution` bindt:

- exact source-, provider- en product-ID;
- de SHA-256 van de afgeleide catalogusdescriptor;
- één status met een begrensde reason code waar vereist;
- exact één applicability-rij voor ieder cycletarget;
- een type-specifieke payload;
- de SHA-256 van de volledige canonieke bijdrage.

Providerbytes worden niet in de cycle gekopieerd. Een `EvidenceReference`
bindt één immutable artefact via `content_sha256`, `receipt_sha256`, rol,
schema en bytegrootte. Daardoor kan de volgende verwerkingslaag exact het
bedoelde decoderresultaat of de bedoelde targetbatch openen en opnieuw
verifiëren zonder een los pad of een providernaam te vertrouwen.

De payloads zijn niet onderling uitwisselbaar:

| Soort | Verplichte velden bij `current`/`stale` |
| --- | --- |
| Radar | observatietijd, bbox, quantity, unit, sampledekking, evidence |
| Satellite | observatietijd, bbox, platform, quantity, unit, sampledekking, evidence |
| Lightning | eventvenster, event- en gapcount, evidence; `absence_claimed` blijft altijd `false` |
| Ground | observatievenster, station- en observatiecount, quantities, evidence |
| Model | modelrun, validiteitsvenster, leadcount, variabelen, evidence |
| Validation | referentietijd, recordcount, validation-role, evidence |

`error`, `blocked` en `not_applicable` gebruiken nog steeds de juiste
typepayload, maar die payload moet leeg zijn. Zo kan een foutstatus niet
tegelijk heimelijk als data-input dienen. `current` en `stale` moeten data en
een `fresh_until_ms` hebben. De cyclegrens controleert `fresh_until_ms` tegen
`cycle_completed_at_ms`.

Bliksemdata claimt op deze grens nooit afwezigheid. Nul events met een geldig
venster kan een invoerobservatie zijn, maar is geen bewijs dat er geen bliksem
was. Vooral voor QoS-0/event-ingress zou die afleiding onveilig zijn.

## Applicability

Elke bronbijdrage bevat voor elk target:

- `country_assessment`: `match`, `mismatch`, `unknown` of `not_constrained`;
- `bbox_assessment`: `match`, `mismatch` of `not_constrained`;
- de uiteindelijke boolean `applicable`;
- een vaste, machineleesbare reden.

De beslisvolgorde is fail-closed:

1. een unavailable target is niet toepasselijk;
2. een expliciete country-match is toepasselijk;
3. een expliciete country-mismatch is niet toepasselijk, ook bij bbox-overlap;
4. bij onbekend country mag alleen een bbox-match de bron toepasselijk maken;
5. zonder countryrestrictie beslist de bbox;
6. zonder country en bbox is de bron globaal.

Een bron die voor geen enkel target toepasselijk is, moet
`not_applicable/target_outside_catalog_scope` zijn. Een bron die voor minstens
één target toepasselijk is, mag die status niet gebruiken. Een upstream
cataloggeblokkeerde bron moet bij toepasselijkheid exact `blocked` met de
catalogusreden zijn.

## Complete cycle en grenzen

De cycle bindt desired-state SHA-256, authority epoch, producer, oplopende
sequence, tijdvenster, targets, bijdragen, catalogushash en een eigen SHA-256.
De dekking wordt opnieuw berekend en bevat onder andere source-, target-,
applicability-, evidence- en statuscounts. Een aangeleverde samenvatting wordt
niet vertrouwd.

Vaste bovengrenzen:

- maximaal 64 targets;
- exact 28 bronbijdragen;
- maximaal 128 evidencereferenties per bron;
- maximaal 2.048 evidencereferenties per cycle;
- maximaal 512 MiB per individuele artefactverwijzing;
- maximaal 2 GiB gerefereerde content per bron;
- maximaal 4 MiB canonieke cycledocumentgrootte;
- maximaal 24 uur tussen cycle-start en -completion.

De byteparser accepteert alleen exact canonieke ASCII-JSON, weigert duplicate
JSON keys vóór objectconstructie, valideert ieder genest schema exact en
herberekent target-, bijdrage- en cyclehashes. Mapping-input wordt naar dezelfde
immutable dataclasses geparsed. Onbekende velden worden niet genegeerd.

## Bewuste volgende grens

Deze cycle is een inputfactlaag, niet het eindproduct. De volgende stap moet de
type-specifieke evidence openen, bronstatussen en targetbijdragen combineren en
pas na afzonderlijke meteorologische admission operationele huidige neerslag,
voorspellingen of waarschuwingen publiceren. De cycle maakt daarbij één eis
afdwingbaar: alle toepasselijke bronnen leveren data of een zichtbare reden
waarom dat deze cycle niet kon.
