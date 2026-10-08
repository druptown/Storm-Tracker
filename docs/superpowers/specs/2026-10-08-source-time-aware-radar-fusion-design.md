# Radarvoorspelling met afzonderlijke bronklokken

Datum: 8 oktober 2026. De gebruiker heeft de gespreksrichting goedgekeurd en
expliciet gevraagd na dit ontwerp direct te implementeren en naar NAS-productie
door te gaan. Geen afzonderlijke akkoordronde voor het uitvoeringsplan.

## Doel

Bereken beweging uit opeenvolgende producten van dezelfde radarbron. Voorspel
het volledige kwantitatieve regenveld, inclusief zwakke regen, naar dezelfde
geldigheidstijden. Combineer pas daarna geschikte bronuitkomsten. Behoud
afzonderlijke intensiteitskernen en maak onverklaarde bronconflicten zichtbaar.
Een gecombineerd beeld krijgt nooit de schijn dat alle pixels op het tijdstip
van de nieuwste bron gemeten zijn.

Dit vervangt de tijdnormalisatie en de primaire route van gefuseerd beeld naar
bewegingsschatting uit het ontwerp van 25 september. Ruimtelijke projectie,
regio-isolatie en geldige nul versus nodata blijven behouden. Het vervangt ook
de uniforme envelopvector uit het regenperiodeontwerp van 7 oktober door
brongebonden lokale veldbeweging; episodecontinuïteit, censoring en afzonderlijke
componentvensters blijven de bedoelde consumenten. De volledige episode-UI is
geen voorwaarde om de bronklokfout te herstellen en wordt niet opnieuw gebouwd.

## Vastgestelde fouten

De live bronbasis is worker 1.18.105, commit `ecd2d49`. In
`thin_regional_radar_fusion.py` worden ruimtelijk geprojecteerde bronnen van
verschillende producttijden gewogen gecombineerd, zonder temporele advectie.
Het resultaat krijgt de nieuwste deelnemende producttijd. Vervolgens schat
`thin_radar_tracking.py` beweging op deze composieten. Lokale oude pixels kunnen
daardoor een verkeerde tijdafstand krijgen; dit bewijst niet dat alle waarden
fout zijn of dat dit de enige oorzaak van de slechte voorspelling is.

Bij Oldenburg bevatte het composiet van 01:25 KNMI 01:20 en Météo-France 01:25.
Een volgend composiet van 01:30 bevatte KNMI 01:30. De lokale KNMI-tijdafstand
was tien minuten, niet de vijf minuten tussen composietlabels.

Daarnaast gebruikte de eerste offline veldproef gelijkheid van dynamische
bijdragerbits als bronidentiteit. Die bits verwijzen naar de laagvolgorde van
één frame, niet naar blijvende providers. Dat defect zit in die proef; de
gearchiveerde bronarrays en oorspronkelijke uitgiften blijven intact.

## Verwerkingscontract

De keten wordt: native product en ontvangst → brongebonden regionale geschiedenis
→ eigen beweging → voorspellingen op gemeenschappelijke tijden → conflictselectie
→ targetregenverloop en componentdetails. Op de tijdgelijk voorspelde combinatie
wordt geen tweede beweging geschat en zij wordt niet opnieuw geadveceerd.

### Identiteit en tijd

Iedere gebruikte invoer behoudt provider, product, verwerking, quantity, eenheid,
meetvenster, echte producttijd, daadwerkelijke ontvangst/beschikbaarheid,
native grididentiteit en onveranderlijke archiefreferentie. Een bitmasker krijgt
een expliciete index-naar-bronmapping per frame. Een alfabetische providerlijst
is daarvoor geen vervanging.

Beweging gebruikt alleen unieke opeenvolgende producten van dezelfde geschikte
provider/product/verwerking en compatibele grids, met hun werkelijke positieve
tijdsafstand. Een herhaald product levert geen nieuwe bewegingsondersteuning.
Onbekende of toekomstige producttijd, toekomstige ontvangst, incompatibele
quantity of een te grote tijdsprong geeft een expliciete abstentie. Bestaande
bronversheidsgrenzen worden niet versoepeld.

### Volledig regenveld en lokale beweging

Gebruik het volledige geldige QPE-veld in mm/u, niet uitsluitend pixels van
objecten boven de shield-drempel. Schat ruimtelijk verdeelde beweging met de
bestaande sparse pyramidal Lucas–Kanade-component. Lokale interpolatie vereist
meerdere toegelaten vectoren en begrensde spreiding en afstand. Niet-ondersteunde
delen blijven onbekend; de vector van één kern geldt niet automatisch voor de
hele regenzone. De begrensde Oldenburg-veldproef is het uitgangspunt voor gerichte
productietests, geen reeds gekwalificeerde productie-implementatie.

Bereken per bron een targetserie via terugwaartse trajecten in het lokale
metrische grid. Advectie omvat zowel bronouderdom als toekomstige lead. Bij
uniforme beweging is het bronpunt `x_target - v * (t_valid - t_product)`.
Een KNMI-product 01:20 en een ander product 01:25 worden voor 01:35 dus
respectievelijk vijftien en tien minuten verplaatst, niet alleen herlabeld.

Behoud aanvankelijk de bestaande horizon van 7200 seconden en tijdstap van
60 seconden. Het nul-leadpunt van een geëxtrapoleerde oude bron is een
schatting voor nu, geen nieuwe meting. Ontbrekende beweging, nodata, verlaten
dekking en onbetrouwbare support zijn onbekend, nooit modeldroog. Positieve
QPE onder 0,1 mm/u wordt afzonderlijk vermeld; de drempel wordt niet op deze
ene passage afgesteld.

De bestaande intensiteitskernen blijven per bron met hun eigen identiteit,
beweging, lokale QPE en passage beschikbaar. Publicatie kan dubbel waargenomen
kernen associëren met behoud van hun bronlineage; zonder eenduidige associatie
blijven het alternatieven. Intensiteiten van parent/child of meerdere radars
worden niet opgeteld. Het volledige veld bepaalt aankomst en einde van het
regenverloop; een componentinterval is geen totale regenduur.

### Conflictregels

Vergelijk alleen dezelfde geografische positie, geldigheidstijd en compatibele
quantity en meetvensters. Een accumulatie wordt niet ongemerkt als instantane
intensiteit behandeld. Aantoonbaar ongeldige of te oude invoer valt af met reden.
Ontbrekende dekking van een bron is geen tegenspraak met een andere bron.

De eerste productieroute is conservatief: behoud bronkandidaten, selecteer een
primaire uitkomst uitsluitend met een vastgelegde kwaliteitsreden en gebruik
andere bronnen voor toetsing of aanvullende dekking. Een deterministische
tiebreak maakt uitvoer reproduceerbaar, maar bewijst geen betere bron. Volgens
de aanvullende gebruikerskeuze blijft ook bij tegenspraak één primaire
schatting zichtbaar met `source_conflict`, in plaats van een leeg resultaat.
Behoud de eerdere primaire bron zolang die geschikt en ondersteund blijft;
bij noodzakelijke nieuwe selectie gelden beschikbare native resolutie en
productactualiteit als expliciet benoemde operationele kwaliteitsproxy's,
daarna bron-ID als deterministische tiebreak. Dit is geen bewijs dat de
gekozen bron meteorologisch gelijk heeft. Kandidaten blijven in provenance.
Geen willekeurig nat-maximum, natte unie of gemiddelde van aankomsten.

Voor numerieke menging van kleine verschillen zijn expliciete, onderbouwde
product-/resolutie-/bewegingsonzekerheidsgrenzen nodig. Zolang zulke grenzen
niet beschikbaar zijn, wordt niet ongemerkt aangenomen dat verschillen klein
genoeg zijn. Bestaande supportgewichten zijn geen gekalibreerde kansen.
De bronspreiding is ook geen 90%-interval. Producten met gedeelde upstreamradars
tellen niet als onafhankelijke bevestigingen; onbekende afhankelijkheid wordt
niet als onafhankelijkheid voorgesteld.

Een eventuele primaire bronwissel gebruikt de eigen geschiedenis van de nieuwe
bron, nooit de vorige primaire bron als bewegingsframe. Continuïteit moet bron-
en tijdtraceerbaar blijven. Een hoofdschatting die door conflict onbekend wordt,
wist de oorspronkelijke immutable uitgifte of vorige eindschatting niet.

Het voorspellingskaartje blijft beschikbaar en rustig: primaire schatting,
status en korte onzekerheidsreden. Wanneer alle verse bewegingsondersteuning
ontbreekt, blijft de laatste passende schatting zichtbaar als niet actueel,
met originele uitgiftetijd en ouderdom. Na verplaatsing wordt geen schatting
van de oude positie hergebruikt. Zonder eerdere schatting of invoer wordt
onbekende timing uitgelegd, niet verzonnen. Beschikbare presentatie is geen
garantie op altijd beschikbare verse meteorologische kennis.

Aanvullende gebruikersvereiste: behoud het bruikbare vroegere concept van een
tijdschatting met een numerieke onzekerheidsindicatie, in plaats van leegte.
Passagekans (raakt de regen het target?) en tijdnauwkeurigheid zijn twee
verschillende grootheden. De oude `forecast_confidence_percent` was een
heuristiek: basis 82/64, framebonus en horizonstraf; geen gemeten trefkans.
Een eventuele identieke score blijft daarom expliciet een ongekalibreerde
betrouwbaarheidsschatting, niet `calibrated_probability_percent`. Bij een
onbekende achterrand wordt de werkelijk ondersteunde ondergrens zichtbaar:
"regen verwacht tot minstens …", niet een verzonnen exact einde. Bij een
bestaande eindschatting zonder verse bevestiging blijven de oorspronkelijke
tijd en ouderdom zichtbaar als eerdere/indicatieve schatting. Kalibratie van
een echte passagekans vereist meerdere onafhankelijke regen- én niet-passage-
gebeurtenissen; supportfractie en bronstemmen zijn op zichzelf geen trefkans.

### Waarneming en voorspelling

De bestaande radarwaarnemingskaart mag ruimtelijk samengestelde bronpixels tonen,
mits hun bron- en producttijden behouden blijven. Zij levert geen tijdhomogene
invoer voor beweging. Een tijdgelijk geadveceerd beeld heet expliciet voorspeld.
Actuele gemeten echo, geëxtrapoleerde huidige regen en toekomstige passage zijn
afzonderlijke toestanden. Geen mm/u uit RainViewer-kleuren of modelwaarde als
gemeten radar presenteren.

## Runtime en opslag

Projectie en bewegingsberekening worden gedeeld per regio-revisie, bron,
productpaar, grid en policyversie, niet opnieuw uitgevoerd voor iedere persoon.
Targettrajecten en continuïteitsstate blijven positiegebonden. Een verplaatste
fictieve tracker verandert geen Belgische targetgeschiedenis. Cache-invalidatie
volgt generation, regio-revisie, grid en productidentiteit. Geheugen en rekenwerk
zijn begrensd; overbelasting geeft expliciete ontbrekende support, geen fallback
naar de foutieve samengestelde bronklok.

Behoud `dedicated_nas_fast`, CPU 200%, nice 0, twee rekenthreads en normale
provider-cadans. Geen extra polling, dependencies, infrastructuur of credentials.
Voorspel targetseries op aanvraag; bewaar niet automatisch 121 volledige
toekomstbeelden per uitgifte.

Gebruik de bestaande PostGIS-archivering met maximaal één kalendermaand
meteorologische historiek. Bewaar bij issues de werkelijk gebruikte native
veldreferenties, eigen product- en ontvangsttijden, bewegingssupport, effectieve
policy/codeversie, bronkandidaten, selectie/conflictreden en target/regiorevisie.
Oorspronkelijke forecasts blijven immutable. Nieuwe bindings zijn additief;
geen herschrijven van oude issues, geen verzonnen historische ontvangsttijd,
geen herhaling van het reeds uitgevoerde opslagherstel. DB-uitval verhindert
geen live publicatie, maar replayvolledigheid blijft dan expliciet beperkt.

## Gerichte verificatie en productiepoort

Technische acceptatie vereist eerst falende en daarna groene regressies voor:

- Eigen producttijden bij tien versus vijf minuten; bronwissel/naad maakt geen
  kunstmatige snelheid en gewijzigde bijdragerbits veranderen geen bronidentiteit.
- Herhaalde producten, toekomstige ontvangst/producttijd, grid-/quantitywissel,
  stale bron, echte nul, nodata en gedeeltelijke bewegingssupport.
- Zwakke achtergrondregen buiten bestaande celmaskers, lokale terugtrace met
  bronouderdom, onbekend einde en echte ondersteunde droge onderbrekingen.
- Tegenspraak tussen nat/droog of aankomsttijden blijft bronalternatief/conflict;
  niet gemiddelde, som of gekalibreerde kans. Gedeelde radarafkomst telt niet dubbel.
- Colocated targets gebruiken dezelfde meteorologie; regioverplaatsing lekt
  geen beweging/stations/state naar andere targets. Dubbele ticks herrekenen
  geen identiek bronpaar; capaciteit en MQTT-schema's blijven begrensd.
- Opslag/readback koppelt iedere uitgifte aan de juiste gebruikte bronnen,
  tijden en policy; ontbrekende opname blijft een expliciet replaygat.

De al lopende gecorrigeerde Oldenburg-proef wordt niet opnieuw gestart. Zij
vergelijkt nieuwe KNMI-voorspellingen causaal met later geregistreerde native
radar en oorspronkelijke productie-uitgiften als baseline. De oude uitgiften
zijn geen waarheid en begrenzen de nieuwe methode niet. Rapporteer aankomst,
einde, stabiliteit, ontbrekende waarschuwingen, foutieve natverwachtingen,
support en runtime afzonderlijk, voor >0 en >=0,1 mm/u. Grondbegin op de exacte
targetpositie is onbekend; nabije gauges zijn andere meetposities.

Oldenburg kwalificeert niet vanzelf meerbronfusie: Météo-France had daar nodata
en de aanwezige DWD-reeks eindigde vóór de hoofdpassage. Gerichte synthetische
overlapcases bewijzen tijd- en conflictinvarianten; zij bewijzen geen meteo-skill.
Gebruik echte overlap bij de normale live nacontrole waar beschikbaar en
claim anders die beperking. Een samengestelde prognose moet aantoonbaar iets
toevoegen tegenover geschikte afzonderlijke bronnen; geen blanket fusion- of
90%-nauwkeurigheidsclaim op één developmentcase.

Na technische acceptatie volgt uitsluitend NAS-HA-deploy volgens
`docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md`, met rollbackbackup, gevalideerde
archieven/hashes, verse workerheartbeat/admission vóór integratie en nacontrole
van gewone unieke producttijden, echte bronbindings, kaart/MQTT en PostGIS.
Geen Core/VM-herstart, persoonlijke trackerwijziging of brede historische suite.
Een geïnstalleerde versie zonder deze nacontrole heet niet gekwalificeerd.

## Wetenschappelijke basis en grenzen

Beweging uit een radarreeks en daaropvolgende veldadvectie zijn gangbare
nowcastingbouwstenen. Groei, verval en initiatie beperken de voorspelbaarheid;
het gebruik van een coherent composiet is op zichzelf niet fout.
[Pulkkinen et al. 2019](https://gmd.copernicus.org/articles/12/4185/2019/gmd-12-4185-2019.html)
beschrijven deze bouwstenen en beperkingen, niet onze specifieke conflictpolicy.

Radarproducten kunnen verschillen in scanning, timing en dekking; bruikbare
composieten vereisen kwaliteitsbeoordeling.
[EUMETNET Open Radar Data](https://eumetnet.github.io/openradardata-documentation/1-ORD-API-overview/)
beschrijft deze bronheterogeniteit. Eerst per bron voorspellen en daarna
conflictbewust combineren is hier onze ontwerpkeuze, geen garantie uit dat
document. S-PROG/STEPS, ensembles, adaptieve 200-km-horizon en empirische
kanskalibratie vallen buiten deze gerichte wijziging.
