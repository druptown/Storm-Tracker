# P0-G01 — verliesloze radarfootprint-topologie

Datum lokaal bewijs: 2026-07-30

## Contract

De autoritatieve footprint is voortaan een immutable, getypeerde structuur:

`MultiPolygon -> Polygon -> (outer ring, hole ring, ...) -> (lat, lon)`

In Python:

```text
GeoPoint = tuple[float, float]
LinearRing = tuple[GeoPoint, ...]
FootprintPolygon = tuple[LinearRing, ...]
FootprintMultiPolygon = tuple[FootprintPolygon, ...]
```

`Observation.footprint_polygons` en
`Observation.parent_footprint_polygons` bevatten deze topologie. De bestaande
`footprint_points`-velden blijven additief aanwezig voor oude producers,
opgeslagen payloads en afnemers. Als de gestructureerde vorm aanwezig is, is
zij autoritatief. Ongeldige gestructureerde ringen worden niet stil
gerepareerd of verwijderd.

## Behouden keten

`providers/raster_components.py` traceert niet langer alleen de grootste lus.
Alle buitenringen en binnenringen worden bewaard. Binnenringen worden aan de
kleinste omvattende buitenring gekoppeld. Een oppervlakcontrole vergelijkt de
som van getekende lusoppervlakken met het werkelijke aantal bronpixels. Als
contourtracing ooit geen sluitend resultaat geeft, blijft iedere bronpixel als
exact afzonderlijk vierkant bewaard; de component of zijn geometrie verdwijnt
dan niet stil.

De structuur wordt doorgegeven door:

- AEMET-radar;
- DPC-radar;
- DWD RADOLAN, zowel lokaal als via de worker;
- H SAF H40B;
- KMI;
- KNMI;
- NOAA GOES RRQPE;
- generieke ODIM HDF5-adapters;
- RainViewer;
- NOAA Enterprise Rain Rate via de worker.

RainViewer bewaart bij tile-overgangen ieder component als een eigen polygon
in de parent-MultiPolygon. De oude samengevoegde puntenreeks blijft alleen een
compatibiliteitsprojectie.

OPERA bewaart nu eveneens de oude vlakke `footprint_points` voor bestaande
afnemers, maar produceert daarnaast autoritatieve
`footprint_polygons`/`parent_footprint_polygons`. De afzonderlijke HA- en
workertracers gebruiken dezelfde strikte rastertopologie: buitenring en alle
droge binnenringen blijven behouden; zuiver diagonaal hoekcontact en andere
ambigue contouren leveren fail-closed geen gestructureerde topologie op.
Workerartifacten bevatten additieve ring- en puntaantallen. HA herberekent en
verifieert die aantallen, naast digest, gecomprimeerde bestandsgrootte en de
bestaande bovengrens van 32 MiB. Oude workerartifacten zonder de twee nieuwe
metadatawaarden blijven leesbaar.

Vervolgens blijft dezelfde topologie behouden in:

- `Observation`;
- `RadarCellSnapshot`;
- `RadarSystemFrame`;
- parent-system state;
- MCS/tracking snapshots en restart-roundtrips;
- HA-naar-worker trackingpayloads;
- duurzame observation-serialisatie.

De HA- en workerbestanden `observation.py`, `storm.py` en `storm_engine.py`
zijn byte-identiek.

## Meteorologische semantiek

Grondbevestiging gebruikt de volledige MultiPolygon. Een regenstation in een
voldoende grote droge binnenring bevestigt de radarneerslag niet; een station
in het werkelijk bezette deel van dezelfde buitenring wel. Afstand tot een
tracker en verschoven footprintprojectie behouden eveneens polygononderdelen
en holes. Een convex hull blijft uitsluitend een afzonderlijke,
vereenvoudigde stormsamenvatting en vervangt de bronfootprint niet.

Bij lage-kwaliteit OPERA-corroboratie die slechts losse bevestigde punten
oplevert, wordt de oude volledige polygon expliciet leeggemaakt. Zo wordt een
puntensteekproef niet ten onrechte als een betrouwbare vlakcontour voorgesteld.

## Lokaal bewijs

Uitgevoerd met `-W error`:

- gecombineerde raster-, provider-, storm-, tracking-, durable- en
  radar-policyselectie: `241 passed, 1 skipped`;
- gerichte OPERA/worker/provider/integratie/radar-policyselectie na de
  OPERA-uitbreiding: `101 passed`;
- `py_compile` op alle gewijzigde Pythonmodules: geslaagd;
- `git diff --check`: geen whitespacefouten (alleen bestaande CRLF-meldingen).

Regressies dekken:

- één 4-connected rastercomponent met buitenring en droog gat;
- een synthetisch ODIM/OPERA-frame met een echte droge binnenring, met
  identieke HA- en workerstatistieken;
- fail-closed gedrag voor diagonaal hoekcontact en open/te grote OPERA-ringen;
- uitsluiting van overlaypixels en lage-kwaliteit radarcorroboratie binnen een
  gestructureerde droge OPERA-binnenring;
- een actuele echte OPERA-download voor de Belgische slice, inclusief
  gesloten-ringen- en payloadgrenscontrole;
- afzonderlijke RainViewer-componenten als één parent-MultiPolygon;
- immutable typed observationcontract;
- cell/frame snapshot-roundtrip met een gat en twee polygononderdelen;
- HA/worker tracking-roundtrip;
- grondstation in gat versus in bezet radargebied;
- bytepariteit van de drie trackingmodules.

## Expliciete gates

Deze lokale wijziging is niet live gedeployed. De test met een echte actuele
OPERA-slice bewijst geldige doorvoer en grenzen, maar het actuele frame
garandeert niet dat er op dat moment toevallig een hole of meerdere
componenten in België aanwezig waren; die topologie is deterministisch bewezen
met de synthetische ODIM-fixture.

Verder vereist productieacceptatie:

- een bewaard echt OPERA-frame dat aantoonbaar een hole bevat;
- HA-worker restart/recovery met zulke snapshots;
- live controle dat kaartweergave en grondbevestiging dezelfde topologie
  interpreteren;
- een NUC-soak met een zware Europese neerslagsituatie als bewijs dat
  payloadgroei binnen CPU-, MQTT- en opslagbudgetten blijft.
