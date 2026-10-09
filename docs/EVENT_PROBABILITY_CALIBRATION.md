# Event-gebalanceerde probabilitycalibratie

Status: lokaal, pure shadowcode; niet gekoppeld aan Home Assistant, workerjobs
of de operationele forecast.

## Doel

`engine/event_probability_calibration.py` levert het lokale contract voor
`P1-N05`. Het zet uitsluitend een echte passagekans om naar een empirisch
gecalibreerde passagekans. Een providerqualityscore, trackingconfidence,
overlapscore of arbitraire betrouwbaarheidsscore kan via deze module niet als
probability worden ingevoerd.

De module maakt geen waarheid, kiest geen productiedrempel en verklaart een
model niet operationeel geschikt. Zonder voldoende onafhankelijk gelabelde
events wordt geen calibrator gemaakt.

## Bewijscontract per trainingscase

Iedere `PassageCalibrationCase` bevat minimaal:

- een unieke case-, event- en frame-ID;
- exact één vooraf bepaalde partition: `train`, `validation` of `test`;
- de ongecalibreerde passageprobability en een confirmed passage-uitkomst;
- provider, regio, leadtimebin en neerslagthreshold;
- SHA-256 van corpus en event-held-out split;
- verschillende SHA-256-artifacts voor kandidaat en onafhankelijke waarheid;
- expliciete bevestiging dat het label confirmed en de truthbron onafhankelijk
  eligible is.

Kandidaat en truth mogen niet hetzelfde artifact zijn. Ambiguous, unknown,
niet-onafhankelijke of intern afgeleide labels worden geweigerd.

## Eventweging

De calibrator gebruikt Pool Adjacent Violators-isotonic regression. Alle rijen
van één meteorologisch event krijgen samen gewicht één. Een event met honderd
trackervensters kan daardoor niet honderdmaal zo zwaar wegen als een event met
één venster.

De caller moet een `CalibrationPolicy` invullen met:

- minimumaantal verschillende events;
- minimumaantal events met minstens één positieve uitkomst;
- minimumaantal events met minstens één negatieve uitkomst;
- expliciet `event_balanced` als weging;
- expliciet `clipped_linear` als interpolatie;
- een versioned policy-ID.

De software bevat geen productiedefaults voor deze steungrenzen. Onvoldoende
steun geeft `InsufficientCalibrationSupport`, nooit een globaal verzonnen
percentage.

## Strata en fallback

Een `CalibrationScope` specificeert provider, regio, leadtimebin en threshold.
Een dimensie met `None` is bewust gepoold. Exacte en gepoolde modellen kunnen
naast elkaar bestaan.

`calibrate_query()` probeert alleen de immutable calibratortuple in de volgorde
die de caller zelf aanlevert. Die selectievolgorde wordt in het resultaat
bewaard. De module heeft dus geen verborgen voorkeur voor KMI, OPERA,
RainViewer, NOAA, een bepaalde regio of een globaal model.

Als geen opgegeven model matcht, volgt `LookupError`; de probability blijft dan
ongecalibreerd/indeterminate volgens het bovenliggende forecastcontract.

## Provenance en replay

Een model bewaart:

- algorithm- en policyversion;
- exacte scope;
- corpus- en splithash;
- hash van alle canonieke trainingscases;
- alle trainingcase- en event-ID's;
- positieve en negatieve eventsteun;
- de volledige monotone isotonic knots en hun eventgebalanceerde gewichten;
- een content-addressed `calibration_version`.

Dezelfde cases in een andere aanlevervolgorde leveren byte-equivalente
modelinhoud en dezelfde calibrationversion.

## Held-out evaluatie

`evaluate_calibrator_held_out()`:

- accepteert alleen `validation` of `test`, nooit `train`;
- vereist exact hetzelfde corpus en dezelfde vooraf bepaalde split;
- weigert ieder event dat ook in training voorkomt;
- berekent raw en calibrated Brier loss eerst per event en daarna over events;
- schrijft een content-addressed evidencehash;
- rapporteert uitsluitend het verschil, zonder een releasegrens te kiezen.

Een lagere Brier loss in één held-out set is op zichzelf geen
productievrijgave. De preregistratie-, minimumevent-, onzekerheids- en
releasegates uit `EVENT_HELD_OUT_VALIDATION.md` blijven noodzakelijk.

## Lokale testdekking

`tests/test_event_probability_calibration.py` controleert onder andere:

- determinisme en monotoniciteit;
- PAV-pooling;
- gelijke totale eventweging bij sterk ongelijke aantallen cases;
- onvoldoende positieve/negatieve eventsteun;
- partition-, scope-, corpus- en splitfouten;
- onafhankelijke confirmed truth;
- caller-gestuurde exacte/gepoolde selectie;
- ontbreken van een impliciete fallback;
- held-out Brierberekening en eventlekkage;
- byte-identieke HA-/workermodules.

## Nog open voor echte meteorologische inzet

- onafhankelijke passage-uitkomsten voor voldoende verschillende weerevents;
- vooraf bevroren event- en leakagegroepen;
- expliciete supportgrenzen per stratum;
- een preregistreerde vergelijking met raw ensemblefrequentie en baselines;
- betrouwbaarheidsintervallen en relevante seizoens-/regimestrata;
- operationele driftmeting zonder testdata voor tuning te hergebruiken;
- shadowwiring naar het trackerforecastcontract;
- live soak en gefaseerde vrijgave.

Zonder die data blijft de calibratorinfrastructuur gereed, maar bestaan er nog
geen meteorologisch geldige productiecalibrators.
