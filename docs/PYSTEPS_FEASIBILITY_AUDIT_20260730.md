# pySTEPS haalbaarheidsaudit — 2026-07-30

Status: lokaal architectuuronderzoek; geen dependency, image of live add-on
gewijzigd.

## Uitkomst

pySTEPS past inhoudelijk bij de geplande probabilistische veldnowcast, maar het
kan niet veilig worden toegevoegd door alleen `pysteps` in
`requirements.txt` te zetten of `cpu_threads` op zes te zetten.

De actuele worker bevat alleen:

- `paho-mqtt`;
- `numpy`;
- `h5py`;
- `pyproj`.

De officiële pySTEPS-installatiehandleiding noemt daarnaast als verplichte
runtime-afhankelijkheden onder andere `jsonschema`, `matplotlib`, `netCDF4`,
OpenCV, Pillow en SciPy. Dask/toolz en pyFFTW zijn optioneel maar juist
relevant voor parallelisatie en snellere FFT.

Bronnen:

- <https://pysteps.readthedocs.io/en/stable/user_guide/install_pysteps.html>
- <https://pysteps.readthedocs.io/en/stable/generated/pysteps.nowcasts.steps.forecast.html>

De handleiding ondersteunt Python `>=3.11,<3.14`; Python 3.13 is dus volgens
de huidige documentatie binnen de geteste reeks. Dat bewijst nog niet dat alle
benodigde binaire wheels op de Alpine/Home Assistant-base voor `amd64`
beschikbaar zijn.

## Wetenschappelijk invoercontract

STEPS verwacht minimaal:

- `ar_order + 1` neerslagvelden op één regulier raster;
- regelmatige tijdstappen tussen die inputvelden;
- een volledig eindig `2 × m × n` advectieveld;
- expliciete ruimtelijke resolutie en tijdstap wanneer
  bewegingsperturbaties/incremental masking worden gebruikt;
- een caller-chosen ensemblegrootte, random seed en modelparameters.

Daarom mogen de huidige provider-overlays, onregelmatige timestamps,
ontbrekende masks of sparse flowvectors niet rechtstreeks als geldige
STEPS-input worden voorgesteld. De bestaande `AnalysisGrid`,
`nowcast_baselines`, `TrackerForecast` en verificatiecontracten zijn de juiste
voorpoorten, maar de daadwerkelijke raster-/motionadapter ontbreekt nog.

## Waarom zes threads nu geen zes-core job geven

De huidige queue voert één Pythonjob tegelijk uit. De variabele
`STV3_CPU_THREADS=6` stelt alleen bovengrenzen voor OpenMP/BLAS-achtige
bibliotheken in. Zuivere Python, parsercode en algoritmen die zelf geen
threaded kernel aanroepen blijven één core gebruiken.

pySTEPS heeft een aparte `num_workers`-parameter. Volgens de officiële
STEPS-API is die parallelroute van toepassing wanneer Dask of pyFFTW actief
is. Dezelfde documentatie adviseert bij `num_workers > 1` juist
`OMP_NUM_THREADS=1`, om oversubscription en vertraging te vermijden.

Er zijn dus twee verschillende profielen:

1. **één nowcastworker met native librarythreads**
   - `num_workers=1`;
   - begrensde OpenMP/BLAS-threads;
   - eenvoudiger geheugen- en crashgedrag.
2. **meerdere ensemble-/FFT-workers**
   - expliciete Dask/pyFFTW-architectuur;
   - `num_workers` caller-supplied;
   - OpenMP per worker op één;
   - harde jobbrede CPU-/geheugen-/deadlinecontrole.

Die profielen mogen niet tegelijk ongecontroleerd zes maal zes threads
starten.

## Verplichte geïsoleerde buildgate

Vóór een workerimage met pySTEPS kan worden gemaakt:

1. Pin exact pySTEPS, Python en alle transitieve dependencies.
2. Bouw een afzonderlijk lokaal `amd64`-image op dezelfde Home
   Assistant-/Alpine-base.
3. Bewijs imports en een kleine deterministische extrapolatienowcast.
4. Meet imagegrootte, importtijd, RSS, tijdelijke allocatie en runtime op de
   NUC.
5. Vergelijk `num_workers=1`, twee en maximaal het toegestane aantal met
   `OMP_NUM_THREADS=1`.
6. Laat de seriële duurzame queue behouden; paralleliseer alleen de interne
   leden van één toegelaten nowcastjob.
7. Bewijs timeout/crash/restart en dat een gedeeltelijk ensemble nooit als
   complete probability wordt opgeslagen.
8. Sla seed, ensembleleden, parameterfingerprint, inputfield-ID’s,
   motionfield-ID en workerbuild op.
9. Vergelijk event-held-out met Eulerian persistence en de reeds
   geïmplementeerde two-frame constant-motionbaseline.
10. Activeer niets live vóór de volledige NUC-/HA-soak en een vooraf
    geregistreerde skillmarge.

## Temperatuur

Een korte piek tot 100 °C is voor deze NUC door de gebruiker als normaal
bevestigd en is geen zelfstandige fout- of releaseconditie. De buildgate kijkt
naar werkelijk door CPU/OS/hardware gemelde throttling en naar gelijktijdige
verslechtering van runtime, queueleeftijd of Home Assistant-latency.

## Huidige beslissing

Nog geen pySTEPS-dependency toevoegen aan de operationele worker. Eerst het
afzonderlijke image bouwen en benchmarken. Tot dan blijven de pure
deterministische baselines en forecast-/verificatiecontracten de reproduceerbare
referentie; zij claimen geen probabilistische skill.
