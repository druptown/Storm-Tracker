# Provider-handoff shadow-engine

## Status en afbakening

`provider_handoff.py` is een pure, immutable en deterministische
shadow-implementatie voor `P1-P03`. De module:

- is niet verbonden met live providerselectie;
- wijzigt geen bestaande `CellTrack`;
- verwijdert geen radarframe, detectie of associatiekandidaat;
- produceert geen snelheid over een provider-, product-, grid- of
  geolocatiewissel;
- bevat geen meteorologische standaarddrempels;
- kan een event exact opnieuw evalueren vanuit de in het event opgeslagen
  input.

De twee identieke implementaties staan in:

- `custom_components/storm_tracker_v3/engine/provider_handoff.py`;
- `storm_tracker_worker/tracking_core/engine/provider_handoff.py`.

De module is dus een lokaal contract en een replaybare challenger. Zij heeft
geen productie-effect zolang een latere integratiestap haar output niet
expliciet consumeert.

## Waarom een providerwissel geen normaal meetinterval is

Twee opeenvolgende centroidcoördinaten zijn alleen rechtstreeks
vergelijkbaar wanneer zij dezelfde fysieke coördinatenbetekenis hebben. Bij
een providerwissel kunnen onder andere veranderen:

- projectie en grid-origin;
- pixelgrootte en sampling;
- compositing en radardekking;
- geolocatieversie;
- reflectivity-naar-rain-rateconversie;
- missing-data-mask;
- detecteerbare objectvorm en intensiteit.

Een naïeve berekening

```text
v = (target_centroid - source_centroid) / elapsed_time
```

kan daardoor grid- of registratieverschil als meteorologische verplaatsing
interpreteren. De shadow-engine verbiedt die update expliciet via
`allow_cross_handoff_velocity_update = false`.

## Immutable inputcontract

### `HandoffFrameReference`

Per source- en targetframe wordt vastgelegd:

- frame-, provider- en product-ID;
- analysis-grid-ID;
- geolocatie-ID;
- nominale valid time;
- volledig geldigheidsinterval;
- SHA-256 van het inputartefact;
- parserversie.

Een frame is structureel ongeldig wanneer zijn nominale valid time buiten het
opgegeven geldigheidsinterval ligt.

### `HandoffTransformEvidence`

De registratie bevat:

- exacte source- en targetframe-ID;
- exacte source- en targetgrid-ID;
- geldigheidsinterval;
- methode-ID;
- affine 2D-transformatie van sourcemeters naar targetmeters;
- 2x2 registratiecovariantie in `m²`;
- gemeten registratieresidu in meter;
- supportaantal;
- bronreden-codes.

De affine matrix is:

```text
[x_target]   [a00 a01 tx] [x_source]
[y_target] = [a10 a11 ty] [y_source]
                           [   1    ]
```

Wanneer geen bruikbare registratie beschikbaar is, blijven matrix,
registratiecovariantie en registratieresidu expliciet `None`. De engine
vervangt dit nooit door een stil veronderstelde identity-transform.

### `HandoffOverlapEvidence`

Het overlaprecord bewaart:

- alle source- en targetframe-ID's die het venster ondersteunen;
- begin en einde van het overlapvenster;
- aantal gepaarde frames;
- footprintoverlap;
- overlap van geldige data;
- overeenkomst van missing masks;
- afstand tussen intensiteitsquantielen;
- phase-correlation peak ratio;
- hashes van ondersteunende artefacten;
- methode-ID en reden-codes.

Ontbrekende overlapmetrics blijven `None`. Zij worden niet als nulmeting of
positief bewijs geïnterpreteerd.

### `HandoffAssociationEvidence`

Iedere mogelijke track-naar-detectiecontinuïteit blijft afzonderlijk
bewaard, inclusief:

- candidate-, track-, frame- en detectie-ID's;
- source-trackrevision;
- voorspellingstijd;
- ruwe sourcecentroid;
- naar het handofftijdstip voorspelde sourcepositie;
- targetdetectiepositie;
- voorspelde en target-meetcovariantie;
- IoU van de geadveceerde predicted polygon;
- voorgaande association-ID's;
- reden-codes.

De engine verwacht dus een voorspelde trackpositie op targettijd. Zij matcht
niet tegen alleen het oude, ruwe centroid.

### `ProviderHandoffPolicy`

De caller moet iedere wetenschappelijke keuze invullen:

- minimaal aantal gepaarde overlapframes;
- minimale registratiesupport;
- minimale footprint- en valid-data-overlap;
- minimale missing-maskovereenkomst;
- maximale intensiteitsquantielafstand;
- minimale phase-correlation peak ratio;
- maximaal registratieresidu;
- maximaal gestandaardiseerd positieresidu;
- minimale advected-polygon-IoU;
- minimale NIS-marge tussen de beste twee associaties;
- minimaal aantal voorgaande associaties;
- extra productverschilcovariantie;
- expliciete blokkerende evidence-reason-codes.

Er zijn geen ingebouwde drempelwaarden of productonzekerheden. De enige
defaults zijn versie- en veiligheidsconstanten zonder meteorologische
betekenis.

## Positieregistratie en onzekerheidspropagatie

Voor kandidaat \(i\) wordt de voorspelde sourcepositie met de affine
transformatie naar het targetgrid gebracht:

```text
z_pred,target = A * z_pred,source + t
```

De voorspelde covariance wordt fysiek meegetransformeerd:

```text
P_pred,target = A * P_pred,source * A^T
```

De covariance van de target-positiemeting tijdens de handoff is:

```text
R_handoff =
    R_target_detection
  + R_registration
  + R_product_difference
```

De innovatiecovariantie is:

```text
S = P_pred,target + R_handoff
```

Daarna worden het registratieresidu en de genormaliseerde innovatie
berekend:

```text
r   = z_target_detection - z_pred,target
NIS = r^T * S^-1 * r
```

De NIS is een geometrische, onzekerheidsbewuste rankingmaat. Zij is geen
empirisch gekalibreerde kans. De ruwe afstand tussen de twee gridcoördinaten
blijft uitsluitend beschikbaar als
`raw_coordinate_displacement_m_audit_only`.

## Continuïteitsstatus

### `confirmed`

Een track wordt alleen in shadow als `confirmed` gemarkeerd wanneer:

- de transform bestaat en voor beide frametijden geldig is;
- alle callervereisten voor transform en overlap gehaald zijn;
- de leidende kandidaat de callervereisten voor NIS, polygon-IoU en
  association history haalt;
- een aanwezige tweede kandidaat voldoende NIS-marge heeft;
- geen expliciet blokkerende evidence-reason-code aanwezig is;
- de leidende targetdetectie niet tegelijk door een andere track wordt
  geclaimd.

Ook bij `confirmed` blijft een rechtstreekse cross-handoff velocity-update
verboden. De beschikbare handoffmeting is alleen een geregistreerde
positiemeting met vergrote covariance.

### `provisional`

Een bruikbare transform met zwakke, incomplete of ambigue overlap- of
association-evidence leidt tot `provisional`. Alle alternatieven en
tekortredenen blijven aanwezig. Een exacte top-NIS-tie krijgt bewust geen
leidende kandidaat; lexicografische ID-volgorde wordt nooit als verborgen
identiteitsbeslissing gebruikt.

### `indeterminate`

Zonder bruikbare transform kan de engine de coördinaten niet fysiek
registreren. De continuïteit wordt dan `indeterminate`, de motion-update wordt
`no_motion_update`, en alle kandidaten blijven als alternatieven bewaard.

Er bestaat geen status `rejected`. Een extreem residu kan de identiteit
onzeker maken, maar verwijdert niet de detectie of het weer.

## Determinisme en replay

`build_provider_handoff_event(...)`:

1. valideert structurele frame-, grid-, tijd- en kandidaatrelaties;
2. sorteert inputkandidaten canoniek op candidate-ID;
3. berekent alle registratie- en covariantiemetrics;
4. rangschikt kandidaten per track;
5. bewaart topalternatieven;
6. detecteert gedeelde leidende targetdetecties;
7. maakt een SHA-256 over alle frames, evidence, kandidaten, policy en
   algoritmeversie;
8. gebruikt de volledige hash als content-addressed event-ID.

`replay_provider_handoff_event(event)` bouwt het resultaat uitsluitend opnieuw
uit de immutable inputs die in het event zelf staan en vereist exacte
gelijkheid. Daardoor zijn inputvolgorde, processtate en dictionaryvolgorde
niet beslissend.

De referenced raw artefacts blijven nodig om de upstream phase correlation,
mask- en quantielmetrics onafhankelijk opnieuw uit pixels te berekenen. Hun
hashes zitten in het event; de pixels zelf worden niet in dit kleine
state-object gedupliceerd.

## Lokale testdekking

`tests/test_provider_handoff.py` controleert onder meer:

- een grote kunstmatige gridverspringing zonder velocityspike;
- propagatie van registratie- en productcovariantie;
- zwakke overlap als provisional;
- ontbrekende transform als indeterminate;
- volledig behoud van kandidaten;
- input-order-onafhankelijkheid;
- top-tie zonder verborgen ID-keuze;
- targetconflict tussen meerdere tracks;
- caller-controlled blocking reason-codes;
- exacte eventreplay en content-adressering;
- een event zonder kandidaten;
- fail-closed structurele validatie;
- immutable records;
- byte-identieke HA- en workermirrors.

## Nog open voor productie

Deze lokale module bewijst nog niet dat handoffcontinuïteit meteorologisch
beter is dan de bestaande live pipeline. Voor productie blijven minimaal
nodig:

- live, lossless overlapcaptures van echte providerwissels;
- onafhankelijk geverifieerde grid- en geolocatietransformaties;
- regionaal en per providerpaar gekalibreerde covariance en policy;
- held-out vergelijking tegen manueel of extern bevestigde cellineage;
- bewijs dat continuïteitsmetrics beter zijn dan legacy;
- shadow soak met providerwissels mét en zonder overlap;
- expliciete integratie met globale assignment en de recursive
  positietoestand;
- operationele observability en rollbackcriteria.

Tot die gates gehaald zijn, blijft dit uitsluitend shadowcode zonder live
effect.
