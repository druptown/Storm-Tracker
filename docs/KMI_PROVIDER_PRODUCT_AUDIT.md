# KMI-productaudit: bronkwaliteit versus huidige adapter

Status: documentatie- en codeaudit; geen live wijziging of nieuwe provider
geactiveerd.

## Kernconclusie

De huidige extreme of tegenstrijdige Storm Tracker-snelheden bewijzen niet dat
KMI-radardata slecht is. Het KMI beschikt over een dicht Belgisch radarnetwerk
en een kwantitatief, gegeorefereerd RADQPE-product. Onze huidige
`providers/kmi.py` gebruikt echter niet dat native product: hij downloadt een
PNG-achtig beeld uit de mobiele-appanimatie en zet kleuren om naar acht
interne intensiteitsklassen.

De praktische conclusie is daarom:

- KMI blijft een sterke voorkeursbron voor België;
- de huidige appbeeldadapter is bruikbaar voor indicatieve
  neerslagaanwezigheid en ruimtelijke vorm;
- hij mag niet als native kwantitatieve rain rate, reflectivity,
  kwaliteitsveld of Dopplersnelheid worden behandeld;
- de beoogde vNext-adapter moet het officiële gegeorefereerde RADQPE-product
  gebruiken en exact opslaan.

## Officiële productinformatie

Het KMI beschrijft vier operationele radars in België en vermeldt dat zijn
radars de atmosfeer ongeveer iedere vijf minuten bemonsteren. Het basisbeeld
heeft een bereik tot 250–300 km, maar kwantitatieve neerslagintensiteit is
volgens het KMI in het algemeen slechts binnen ongeveer 100 km betrouwbaar.
De beelden worden verwerkt om niet-meteorologische echo's en radarfouten te
corrigeren en intensiteiten aan regenmeters aan te passen:

- [KMI — Radarnetwerk](https://www.meteo.be/nl/over-het-kmi/waarnemingsnetwerk/radarnetwerk)
- [KMI — Welke beelden worden gegenereerd?](https://www.meteo.be/nl/info/veelgestelde-vragen/radar/wat-zijn-gegenereerde-beelden)

De mobiele KMI-app beschrijft zijn radaranimatie als een presentatie met
tijdstappen van tien minuten en een verwachting tot drie uur. Dat is een
gebruikersproduct, niet automatisch het beste machineleesbare
observatieproduct:

- [KMI — Mobiele app](https://www.meteo.be/nl/diensten/producten-en-diensten/mobiele-app)

Het officiële RADQPE-project levert real-time kwantitatieve
radarneerslagschattingen voor België en omgeving:

- [KMI — RADQPE](https://www.meteo.be/en/projects/radqpe)
- [KMI — RADQPE User Guide](https://radli.meteo.be/uploads/media/63d3e7525ab68/radqpe-userguide.pdf?v20241107-0827=)

Volgens de user guide heeft het RADQPE-composiet:

- Belgian Lambert 2008 als projectie;
- een gridresolutie van 1 km;
- een rain-ratecomposiet om de vijf minuten, met maximaal ongeveer vijf
  minuten productvertraging;
- 5- en 10-minutenaccumulaties om de vijf minuten, plus 1 uur en 24 uur;
- GeoTIFF en ODIM-HDF5 als machineleesbare formaten;
- expliciete grid-, tijd-, product-, quantity-, gain-, offset-, nodata- en
  processingmetadata;
- radar-QC, reflectivity-naar-rain-rateconversie en gauge-based
  biascorrecties;
- dagelijkse verificatie tegen een apart klimatologisch regenmeternetwerk dat
  niet in de productberekening wordt gebruikt.

Ook dit product is niet foutloos: de guide benoemt heterogene radartechniek,
mogelijke informatieverliezen door processing, variabele gauge-QC en
best-effortbeschikbaarheid. Die beperkingen moeten als productmetadata en
qualityflags worden bewaard, niet verborgen achter één vaste score.

## Wat de huidige code werkelijk doet

`custom_components/storm_tracker_v3/providers/kmi.py`:

1. roept de mobiele-appservice `getForecasts` aan voor één locatie;
2. kiest het laatste niet-toekomstige item uit de animatiesequentie;
3. downloadt het bijbehorende beeld;
4. decodeert het beeld naar RGBA;
5. kiest voor iedere pixel de dichtstbijzijnde kleur uit acht hardgecodeerde
   kleurklassen;
6. projecteert pixels lineair binnen hardgecodeerde latitude/longitudegrenzen;
7. maakt connected components en intensity-runs;
8. geeft iedere component een vaste quality `0.95`;
9. bewaart in de legacy calibratieroute de afgeleide overlay, niet de
   gedownloade beeldbytes.

Daaruit volgen concrete beperkingen:

- de kleurklasse 1–8 is geen gedocumenteerde fysieke eenheid;
- anti-aliasing, transparantie, legenda- of kaartwijzigingen kunnen de
  kleurklassificatie beïnvloeden;
- een lineair lat/lonbeeld is niet gelijk aan de native radarprojectie;
- een vaste quality `0.95` is geen upstream quality index;
- native nodata, clutter-, beamblockage- en gauge-QC verdwijnen;
- de appsequence kan observatie- en forecastbeelden mengen als metadata niet
  volledig wordt gecertificeerd;
- de legacy “original frame”-tabel bevat voor KMI een afgeleide overlay;
- centroidverplaatsing van de gedetecteerde kleurcomponent is geen
  Dopplersnelheid en geen onafhankelijke celidentiteit.

## Correct vNext-providercontract

De nieuwe KMI-route moet als afzonderlijke provider worden gebouwd; de
bestaande adapter blijft alleen een duidelijk gelabelde tijdelijke
visualisation fallback.

Minimaal contract voor `kmi_radqpe`:

1. gebruik het officiële GeoTIFF- of ODIM-HDF5-product;
2. leg productnaam en quantity vast: rain rate of exact accumulationtype;
3. gebruik `startdate/starttime/enddate/endtime` en UTC-semantiek uit het
   artifact;
4. bewaar Lambert 2008/PROJ-definitie, native extent, x/y-scale en
   rasterafmetingen;
5. decodeer gain, offset, nodata en undetect exact;
6. bewaar alle aanwezige `how`-/processing-/qualitymetadata;
7. sla de ontvangen bytes vóór parsing content-addressed op met URL/key,
   fetch-/receipt time, HTTP-status en relevante secretvrije headers;
8. maak een native `RadarFrame` en pas daarna een afzonderlijke analysis-grid
   view toe;
9. houd rain rate, accumulation, reflectivity en visualisation categorisch
   gescheiden;
10. laat data buiten de kwantitatief betrouwbare afstand bestaan, maar label
    de grotere meetonzekerheid;
11. bewaar providerfouten en ontbrekende frames; vervang ze niet door
    nulneerslag;
12. test met officiële voorbeeldbestanden en vaste SHA-256-fixtures;
13. vergelijk appbeeld en RADQPE alleen als twee verschillende producten van
    dezelfde organisatie;
14. maak pas een operationele voorkeur na event-held-out replay en live shadow
    soak.

## Wat deze audit wel en niet bewijst

De officiële specificaties tonen dat er een veel rijker KMI-product bestaat
dan onze huidige kleurenadapter benut. Ze bewijzen niet dat ieder individueel
frame foutloos is en leveren niet vanzelf onafhankelijke celidentiteit of
groundtruth voor Storm Tracker.

De juiste engineeringbeslissing is daarom niet “KMI afkeuren”, maar:

- het native KMI-product correct capturen;
- onzekerheid en afstandsbeperkingen bewaren;
- motion, associatie en passage onafhankelijk valideren;
- het appbeeld nooit meer als kwantitatief origineel presenteren.

De concrete, fail-closed implementatie en de exacte capture van de brede
regenzone van 30 juli 2026 staan in
[`KMI_RADQPE_SHADOW_ADAPTER.md`](KMI_RADQPE_SHADOW_ADAPTER.md). Dat document
houdt het gedocumenteerde native `kmi_radqpe_native` strikt gescheiden van de
publiek bereikbare maar meteorologisch nog onvoldoende gespecificeerde
`kmi_public_radar_composite`.
