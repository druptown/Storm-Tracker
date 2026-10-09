# Gebonden analysis-grid-transformatie (P1-M01 shadow)

Status: volledig lokaal, worker-only, shadow-only en **ongewired**. Deze module
is niet geregistreerd, niet gescheduled, niet in een providerregistry opgenomen,
niet naar Home Assistant gepubliceerd en niet gedeployed.

Implementatie:
`storm_tracker_worker/analysis_grid_transform.py`.

## Harde ingangspoort

De service aanvaardt geen los raster en geen caller-supplied projectienaam. De
ingang bestaat uit:

1. één bestaande `NativeRainRateFieldCandidate` van de canonical radar-field
   bridge;
2. de door de caller vastgelegde SHA-256 van **alle** kandidaatprovenance,
   native arrays, maskers, QC-planes en optionele redundante velden;
3. de exacte SHA-256 van het reeds geselecteerde meteorologische
   semanticscertificaat;
4. één expliciet caller-supplied `AnalysisGrid`;
5. een afzonderlijke transformadapter met een volledig gebonden certificaat;
6. een expliciete methode-/tolerantie-/capaciteitsconfiguratie.

`native_candidate_content_sha256()` wordt vóór en na de berekening opnieuw
berekend. Dat is nodig omdat een read-only NumPy-view nog een elders beheerde,
schrijfbare base kan hebben. Een wijziging vóór de start geeft
`source_candidate_checksum_mismatch`; een wijziging tijdens de berekening geeft
`source_candidate_changed_during_transform`. In beide gevallen bestaat geen
gedeeltelijk outputveld.

De bron moet nog exact `candidate_only_unadmitted` zijn en mag zelf geen
reprojectie, resampling, threshold of wet/dry-classificatie bevatten. Alleen
een reeds gecertificeerde rate in `mm/h` wordt aanvaard. Accumulatie-naar-rate,
DBZH-naar-rate of providerfallback horen niet in deze laag.

## Geen impliciete CRS-logica

De module importeert geen `pyproj`, GDAL of providerdecoder. Ze leest geen CRS-
database en kiest geen asvolgorde. De eerste gesloten adapter ondersteunt
uitsluitend:

```text
mapping_kind = certified_axis_aligned_pixel_edges
source columns = west_to_east
source rows    = north_to_south
target axes   = x_east_y_north
target unit   = metre
```

Het certificaat bindt exact:

- provider, product en bridge-algoritme;
- bron-semanticscertificaat;
- native CRS, griddefinitie, coveragedefinitie, afmetingen en array-asvolgorde;
- doel-CRS;
- adapteridentity/-versie en transformconfig-SHA;
- bronpixelrand `(row=0, column=0)` in doelcoördinaten;
- positieve x- en y-pixelsteun in meter;
- bewijsclassificatie, evidence-SHA, authority en eventuele primaire bronnen;
- één exacte methode per behouden QC-plane.

Rotatie, shear, curvilinear grids, unstructured grids en een afgeleide of
gegokte pixelorigin zijn niet representabel in dit certificaat en worden dus
ingehouden. Een synthetisch certificaat mag alleen een providernaam certificeren
die met `synthetic` begint. Een echte provider vereist de aparte klasse
`official_exact_grid_anchors`, een bewijs-SHA, authority en minstens één HTTPS-
primaire bron. Er is bewust nog geen standaardcertificaat voor KNMI, NOAA, Met
Office, DWD, KMI of een andere echte provider.

De adapter levert per verzoek een content-derived
`CertifiedCoordinateTransformReceipt`. Dat receipt bindt bronchecksum,
semanticscertificaat, doelgrid-SHA, transformconfig, methode, support-edges en
adapter-/certificaatfingerprint. Hoeken en centrum krijgen een expliciete
edge-coordinate → target-coordinate → edge-coordinate roundtrip. De service
controleert het receipt onafhankelijk en weigert een overschrijding van de
caller-tolerantie.

Het volledige certificaat, de volledige config en het volledige receipt met
roundtripaudit zitten in het outputobject; niet alleen hun ID.

## Fysische resampling

De enige methode is momenteel:

```text
conservative_area_overlap_rate_v1
```

Voor ieder doelpixel wordt de exacte rechthoekige overlap met bronpixelsteun in
het gecertificeerde metrische coördinatenstelsel berekend. Een rate is een
intensieve grootheid; wanneer de volledige doelpixel uit geldige bronsteun
bestaat, is de output het overlapoppervlakte-gewogen gemiddelde:

```text
R_target = sum(R_source * overlap_area) / target_pixel_area
```

Bij een volledig geldig domein behoudt dit de oppervlakte-integraal van de
rate. Zowel coarsening als refinement gebruikt dezelfde gebiedsregel. Refinement
claimt geen extra meetresolutie: het verdeelt alleen de gecertificeerde,
stukgewijs constante bronpixelsteun.

Er wordt nooit buiten de bewezen source support geëxtrapoleerd. Elke doelpixel
heeft vier expliciete oppervlaktefracties:

- geldig;
- nodata;
- undetect;
- buiten source support.

Deze fracties sommeren deterministisch tot één. Het doelpixel krijgt daarnaast
exact één toestand:

- `valid_mask`: 100% geldige steun, met een finite niet-negatieve rate;
- `nodata_mask`: 100% nodata;
- `undetect_mask`: 100% undetect;
- `mixed_mask`: volledige geografische steun maar gemengde bronstatus;
- `outside_support_mask`: enig niet-bewezen oppervlak buiten bronsteun.

Alle niet-geldige waarden zijn `NaN`, nooit technische nul. Een echte geldige
`0.0 mm/h` blijft wel exact nul. Een gemengde doelpixel wordt niet uit alleen
het geldige deel genormaliseerd: dat zou onbekend oppervlak als impliciet
representatief behandelen.

## QC blijft afzonderlijk

Alle bron-QC-planes worden onveranderd en immutable in het outputobject
behouden, inclusief naam, semantics, dtype en arraychecksum. Daarnaast kan een
doelgrid-QC-plane alleen ontstaan via een exact certificaatrule. De enige eerste
rule is `exact_area_consensus_v1`:

- alle bronpixels met positieve overlap moeten finite zijn;
- hun QC-waarde moet exact gelijk zijn;
- anders is `consensus_mask=False` en wordt geen gegemiddelde QC-waarde
  verzonnen.

Een ontbrekende rule, afwijkende semantics-SHA, andere dtype, objectarray of
onbekende methode houdt de volledige veldtransformatie in. QC wordt nergens tot
een confidencepercentage of meteorologische detectie vertaald.

## Outputbewijs en pixel accounting

`AnalysisGridRainRateFieldCandidate` is frozen en bevat read-only owned arrays.
Het veld blijft `candidate_only_unadmitted` en registreert apart:

- coördinatietransform toegepast;
- werkelijke reprojectie alleen wanneer bron- en doel-CRS verschillen;
- resampling toegepast;
- extrapolatie niet toegepast;
- thresholding niet toegepast;
- wet/dry-classificatie niet toegepast.

De output-SHA omvat het complete doelgrid, alle waarden/maskers/fracties, bron-
en doel-QC, tijden/quantity/unit, methode, pixel accounting, operation flags,
bronbinding, certificaat, config en receipt. `field_id` is exact
`analysis-field-<output-SHA>`. `analysis_grid_field_content_sha256()` kan dit
achteraf onafhankelijk herberekenen.

Pixel accounting bevat:

- totaal en aantallen valid/nodata/undetect/mixed/outside;
- werkelijk aantal overlapbijdragen;
- geldig, nodata, undetect en outside oppervlak in m²;
- totaal doeloppervlak;
- geldige `rate × area`-integraal.

## Resource- en deadlinegrenzen

Harde modulemaxima:

- 4.000.000 bronpixels;
- 4.000.000 doelpixels;
- 512 MiB gematerialiseerde arrays;
- 64.000.000 overlapbijdragen;
- 32 QC-planes;
- één absolute monotone deadline, standaard 30 seconden.

Iedere callerconfig moet strengere of gelijke grenzen kiezen. Voorallocatie
wordt eerst conservatief geraamd en na materialisatie met echte `nbytes`
gecontroleerd. De deadline wordt vóór bewijsvalidatie, receiptuitgifte,
resampling, iedere doelrij, iedere 4.096 overlapbijdragen,
checksumherverificatie en return gecontroleerd. Overlap- en QC-accounting is
constant-memory per doelpixel; een zeer grove doelpixel kan daardoor niet via
miljoenen tijdelijke Python-objecten de array-bytebegrenzing omzeilen.
Dit is coöperatieve bounded execution; CPU/RSS/wallclock-isolatie door een hard
child blijft een afzonderlijke vereiste vóór scheduling.

## Synthetisch testbewijs

`tests/test_worker_analysis_grid_transform.py` dekt:

- exact identity-grid met echte nul, nodata en tijd-/quantitybehoud;
- conservative coarsening en refinement;
- mixed valid/nodata zonder partial renormalization;
- afzonderlijke undetect, nodata en geldige nul;
- gedeeltelijk en volledig buiten source support zonder extrapolatie;
- QC exact-consensus en volledige bron-QC-retentie;
- ontbrekende en afwijkende QC-certificaten;
- curvilinear fail-closed;
- target-CRS-, config-, semantics- en contentchecksumbinding;
- mutatie achter een read-only NumPy-view;
- deterministische receipt-, field-ID- en outputchecksum;
- embedded certificaat/config/roundtripaudit;
- absolute deadline en bron-/doel-/byte-/overlapcapaciteit;
- afwezigheid van impliciete projectie- en live imports;
- verbod op synthetisch bewijs voor een echte provider.

Gerichte gate tijdens implementatie:

```text
tests/test_worker_analysis_grid_transform.py
25 passed, ResourceWarning-as-error
```

## Bewust open en geblokkeerd

Deze stap bewijst de mechaniek uitsluitend met exact-affine synthetische grids.
Voor echte providerreprojectie ontbreken nog per product onder andere:

1. officiële pixel-edge anchors en CRS-/asvolgordebewijs;
2. een gereviewd transformcertificaat met golden coordinate fixtures;
3. onafhankelijke vergelijking met een vertrouwde reprojectionreferentie;
4. beleid voor rotated affine, curvilinear en geographic-area geometry;
5. provider-/regimegebonden QC-resamplingsemantiek;
6. area-integral golden tests op echte producten;
7. held-out meteorologische replay en foutbudget;
8. hard-child en NUC/HA/Z2M thermische/latencysoak.

Zonder die poorten blijft een echt providerveld op zijn native grid en levert
deze service geen doelgridkandidaat.
