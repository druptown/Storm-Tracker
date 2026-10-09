# DWD RV DE1200 native analysis-gridcertificate

Status: **shadow-only, worker-only, ongewired en niet operationeel toegelaten**.
Deze stap geeft voor het eerst een echt providercertificaat uit aan
`analysis_grid_transform.py`, maar uitsluitend voor het ongewijzigde native
DWD-raster en dezelfde native CRS. Er is geen generieke reprojection.

## Wat exact gecertificeerd wordt

Het actuele officiële [DWD RV-HDF5-productprofiel][dwd-rv-hdf5] bindt het
product aan vijfminutenaccumulatie, HDF5, 1 km en 1200 × 1100. De officiële
[DWD RV-formatbeschrijving][dwd-rv] legt voor het WGS84-DE1200-grid vast:

- 1100 pixels west–oost en 1200 pixels noord–zuid;
- cellen van 1 km × 1 km;
- polar stereographic, `lat_0=90`, `lon_0=10`, `lat_ts=60`;
- WGS84-halve assen en de productspecifieke `x_0`/`y_0`;
- `x_0` en `y_0` positioneren het **middelpunt van de eerste
  noordwestpixel**;
- alle vier geografische DE1200-hoeken.

De officiële [EUMETNET OPERA ODIM_H5 2.3-specificatie][odim] vult het ontbrekende
arraycontract in:

- een cartesiaans beeld start linksboven;
- rijen lopen noord→zuid en kolommen west→oost;
- `UL/UR/LL/LR` zijn de buitenhoeken van de respectieve buitenste pixels.

Daaruit volgen in de door DWD verschoven native projectie exact deze
pixelranden:

| rand | meter |
|---|---:|
| west | −500 |
| noord | +500 |
| oost | +1.099.500 |
| zuid | −1.199.500 |

De centra lopen dus van `(0, 0)` voor de noordwestpixel tot
`(1.099.000, −1.199.000)` voor de zuidoostpixel. Dit is consistent met 1100 ×
1200 cellen van exact 1000 meter.

## Volledige bewijsbinding

`storm_tracker_worker/dwd_rv_analysis_grid_certificate.py` geeft alleen een
`AffineGridTransformCertificate` uit als alles hieronder exact overeenkomt:

1. de caller bindt de volledige `NativeRainRateFieldCandidate` met SHA-256;
2. de kandidaat komt uit de gereviewde canonical-radar-field bridge;
3. provider, product, decoder identity/version/config en DWD-semanticscertificaat
   zijn exact;
4. bronhoeveelheid is de vijfminutenaccumulatie in mm en output is de daarvan
   berekende intervalgemiddelde rate in mm/u;
5. het tijdvenster is exact 300 seconden en `valid_at` is het intervaleinde;
6. native grid is `regular`, `yx`, 1100 × 1200;
7. alle vier coverage-randen zijn exact de waarden hierboven;
8. de volledige gesloten DWD-projstring wordt token voor token gevalideerd;
9. arraytypen en de oorspronkelijke `uint32`-plane met sentinels zijn exact;
10. de target gebruikt byte-identiek dezelfde native CRS;
11. grid-, coverage-, semantics-, candidate- en transformconfigchecksums worden
    in het certificaat vastgelegd.

De certificate-ID is zelf afgeleid van candidate, grid, coverage, evidence,
semantics, target-CRS en transformconfig. Een wijziging maakt dus een nieuw
certificaat; ze kan niet stil onder dezelfde fingerprint doorgaan.

## Wat de stap niet claimt

- Geen transform naar Lambert, EPSG:3857 of een Europees gemeenschappelijk
  raster.
- Geen operationele admission, scheduler- of servicekoppeling.
- Geen wet/droogclassificatie, threshold, extrapolatie of snelheidsfilter.
- Geen radar-QC-score: RV heeft in dit contract geen native quality plane.
- Geen claim dat sampling van 1 km meetnauwkeurigheid van 1 km betekent.

Een target-grid in dezelfde CRS mag croppen, verfijnen of vergroven. De
conservative area-overlap transform bewaart daarbij rates; nodata, undetect,
mixed en outside-support blijven afzonderlijk. De oorspronkelijke `uint32`-
plane wordt alleen bij exact oppervlakteconsensus naar een targetpixel
overgenomen.

## Fail-closed tests

De gerichte suite controleert onder andere:

- exacte randen, dimensies, officiële evidenceklasse en checksumketen;
- deterministische certificate-identiteit;
- werkelijk gebruik door de analysis-gridtransform zonder reprojection;
- weigering van provider-, decoder-, CRS-, dimensie-, tijd-, coverage- en
  plane-drift;
- detectie van arraymutatie nadat de caller zijn SHA-binding maakte;
- afwezigheid uit service, scheduler, configuratie en Home Assistant.

KNMI blijft hiervan bewust gescheiden: zijn officiële publicaties spreken
elkaar nog tegen over de ellipsoïde en sluiten de pixelranden niet. Het bestaan
van dit DWD-certificaat mag de fail-closed KNMI-gate nooit openen.

[dwd-rv]: https://www.dwd.de/DE/leistungen/radarprodukte/formatbeschreibung_rv.pdf?__blob=publicationFile&v=3
[dwd-rv-hdf5]: https://www.dwd.de/DE/leistungen/radarprodukte/radarkomposit_rv_hdf5.pdf?__blob=publicationFile&v=3
[odim]: https://www.eumetnet.eu/wp-content/uploads/2019/01/ODIM_H5_v23.pdf
