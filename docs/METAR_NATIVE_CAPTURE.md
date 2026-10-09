# METAR native cache capture

Status: worker-only kandidaat, lokaal getest, niet live gekoppeld en niet
gedeployed.

## Bron en betekenis

De officiële Aviation Weather Center
[`Data API`](https://aviationweather.gov/data/api/) raadt voor grote of
frequente datasets de gzipcache aan. `metars.cache.csv.gz` bevat alle huidige
wereldwijde METARs en wordt eenmaal per minuut bijgewerkt. Het gekozen raw
artifact is daarom de volledige, exacte gzipresponse en niet een plaatselijke
subset of een lijst reeds geïnterpreteerde stations.

METAR is een gecodeerd rapport van een terminalwaarneming. De capture noemt
dit daarom bewust `raw_terminal_observation_reports`, niet reeds
geïnterpreteerde grondwaarheid. Present-weathergroepen kunnen neerslag op het
station melden, maar een ontbrekende groep is niet bij ieder station bewijs
van droog weer. `VC` is nabijheid, `PWINO` meldt een defecte
present-weathersensor, `AO1` heeft geen neerslagdiscriminator en
neerslagaccumulatie sinds het vorige rapport bewijst geen actuele regen. De
native capture doet daarom nog geen meteorologische interpretatie.

## Harde capturevolgorde

`storm_tracker_worker/metar_native.py`:

1. aanvaardt alleen een globale capturejob met geldige job-id en exact lege
   payload, vóór netwerktoegang;
2. weigert redirects en systeemproxy-omleiding en eist exact de vaste HTTPS-
   origin, het vaste pad, HTTP 200, identity transportencoding en een bekende
   cache-`Content-Type` (`application/octet-stream`, `application/gzip` of
   `application/x-gzip`);
3. leest met een compressed-bytegrens, per-I/O-timeout en totale downloaddeadline;
4. bewaart de volledige exacte gzipbody in `RawArtifactStore` schema 3;
5. decomprimeert pas daarna incrementeel, met grenzen voor compressed bytes,
   uitgepakte bytes, expansieratio, totale inspectietijd en fysieke/logische
   CSV-records; er wordt geen volledige uitgepakte kopie of `StringIO` gemaakt;
6. valideert het exacte positionele officiële schema van 44 kolommen. Alleen de
   vier verwachte paren `sky_cover`/`cloud_base_ft_agl` mogen herhaald zijn;
7. valideert exacte rijbreedte, veld- en rijbudget en minimaal één structureel
   bruikbare station/tijd/positie/raw-rapportcombinatie;
8. commit alleen dan een complete capture met required role `csv_gzip`.

Ongeldige gzip, trailing gzipleden, fout CSV-schema, lege/ongeldige records of
resourceoverschrijding laten hoogstens een exact receipt achter. Ze produceren
geen complete capture en nooit een impliciete `0 stations`, `droog` of
`geen regen`.

Retry gebruikt de deterministische receipt- en capture-identiteit. Een reeds
complete capture wordt met capture, receipt en gzipbytes in één
objectverificatie en zonder netwerk opnieuw opgebouwd. Na een crash tussen
raw-store en inspectie worden de duurzaam opgeslagen gzipbytes zonder nieuwe
download geïnspecteerd en gecommit. Een upstreamwijziging kan daardoor een
half-afgewerkte job niet vervangen.

De capture gebruikt nu een absolute monotone jobdeadline over threadlock,
procesoverschrijdende provider-capacity- en slotlock, netwerk/read, inspectie,
opslag en commit. Een
complete retry leest/hash't het ene gzipmember exact eenmaal via de schema-3
bundel-API. Een canonieke, atomair gepubliceerde en in aantal/bytes begrensde
verdict bindt content-SHA, exacte `received_at` en inspectorversie. Dit laatste
is noodzakelijk omdat current/stale/future-diagnostiek van de ontvangsttijd
afhangt. Success en structurele afkeur overleven een store-reopen zonder zware
gzip/CSV-reparse. Timeout wordt expliciet niet gecachet; directory- en
file-fsync plus een proceslock beschermen publicatie en capaciteit.

De globale cache bevat vele onafhankelijke observatietijden. Oudste/nieuwste
gedeclareerde tijd en aantallen current/stale/future/invalid worden alleen als
structurele diagnostiek bewaard met
`timestamp_quality_state=unverified_non_operational`. Ze worden niet valselijk
als producttijd of operationele kwaliteit gebruikt. De capture houdt
`product_timestamp=null`; `weather_state` blijft `unknown`.

De lokale echte AWC-cache `metars_current.csv.gz` is een golden test voor het
volledige capturepad en de herhaalde wolkenkolommen. Een aanvullende live
read-only controle op de officiële URL bevestigde hetzelfde 44-kolommenschema en
`application/octet-stream`. De registryvlag betekent uitsluitend dat deze
geïsoleerde raw-kandidaat getest is; `metar_fetch` blijft result-only en de
service blijft op de bestaande handler staan.

## Nog gesloten

De kandidaat retourneert geen observations en is niet operationeel toegelaten.
Vóór service-omschakeling moeten nog volgen:

- exact aan raw/capture/decoderversie gebonden stationrecords;
- volledige METAR present-weather-, AUTO/AO1/AO2-, `PWINO`- en
  tijdkwaliteitscertificatie, inclusief `VC`, correcties/SPECI en het verschil
  tussen actuele present weather en historische accumulatie;
- stationcatalogus, sensorcapaciteit en expliciete ruimtelijke/tijdelijke
  representativiteit voor een tracker;
- ruimtelijke selectie in de worker zonder wereldcache naar Home Assistant te
  sturen;
- fusie waarbij afwezigheid/onbekend nooit als droog geldt;
- compacte snapshot-only HA-projectie;
- faults, replay, latency, thermische en soaktests.

Gericht testbewijs omvat de echte AWC-golden, exact-byte-before-inspection,
redirect/origin/status/type, gzip/trailing/bombs, exact schema, crash-resume,
timestampdiagnostiek en de expliciete afwezigheid van weerclaims. Testaantallen
worden niet als blijvende productspecificatie gebruikt. De huidige gerichte
stand is `31 passed`, inclusief counted-read, reopen-verdict en transient-
timeoutregressies.
