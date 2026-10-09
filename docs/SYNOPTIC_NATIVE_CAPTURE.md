# Synoptic native Time Series capturekandidaat

Status: worker-only, geïsoleerd, niet gewired en niet gedeployed.

`storm_tracker_worker/synoptic_native.py` is een exacte-raw
capturekandidaat naast de ongewijzigde legacyhandler in `synoptic.py`. De
kandidaat publiceert geen stations, neerslagwaarden, wet/dry-besluit of
grondbevestiging. Ook een structureel geldige response blijft
`operational_admission_eligible: false` en `weather_state: unknown`.

## Officieel broncontract

De request volgt de officiële Synoptic-documentatie:

- Time Series: <https://docs.synopticdata.com/services/time-series>;
- JSON-output: <https://docs.synopticdata.com/services/json-output-format>;
- actuele fout- en code-2-vormen: <https://docs.synopticdata.com/services/errors>;
- public tokens: <https://docs.synopticdata.com/account/public-api-tokens>;
- afgeleide neerslag: <https://docs.synopticdata.com/services/precipitation-service-explained>;
- provider-QC: <https://docs.synopticdata.com/services/mesonet-data-qc>.

Het endpoint is exact
`https://api.synopticdata.com/v2/stations/timeseries`. De request selecteert
alleen neerslag (`vars=precip`, `precip=1`) en vraagt `all_reports=1`,
`sensorvars=1`, `qc=on`, `qc_remove_data=off`, `qc_flags=on` en de drie
QC-bronnen `synopticlabs,madis,mesowest`. `recent=60`, `limit=50` en de
metrische neerslageenheid staan in de canonieke querycontext.

`limit=50` bewijst nooit gebiedsdekking. De kandidaat meldt daarom altijd
`coverage_incomplete` en `area_coverage_evidence: none`. De door Synoptic
afgeleide `precip_intervals` zijn hoeveelheden tussen rapporttijdstippen en
geen instantane `rain_live`-meting.

## Exacte job- en query-identiteit

Een job bevat exact vijf velden: eindige `latitude`, `longitude` en
`radius_km`, plus een korte canonieke `query_key` en `query_kind` (`local` of
`roaming`). Extra velden, inclusief credentials, worden voor netwerk of
opslag geweigerd. `job_id` en `query_key` zijn maximaal 96 tekens, lowercase
ASCII en gebruiken alleen begrensde separators.

Elke aaneengesloten of via `-_.:@/` reconstrueerbare 32-hexidentiteit wordt
geweigerd. Er is bewust **geen UUID-uitzondering**: een UUID bevat eveneens
32 reconstrueerbare hextekens en zou anders een token in UUID-vorm duurzaam
kunnen verbergen. Gebruik voor duurzame IDs een niet-geheime, niet volledig
hexadecimale identiteit.

De canonieke querycontext bevat alle niet-geheime requestsemantiek en een
SHA-256, maar nooit de credential of de volledige Authorization-header.

## Credentialgrens

`STV3_SYNOPTIC_TOKEN` moet exact 32 lowercase hextekens bevatten, conform het
public-tokencontract. Een application key voldoet niet aan dit contract. De
token gaat alleen als `Authorization: Bearer …` mee; de URL bevat geen
`token`-queryparameter. Redirects en environmentproxies zijn uitgeschakeld.

Voor opslag wordt de volledige, reeds door de transportgrenzen begrensde body
fail-closed onderzocht. De scanner vindt de eigen token in:

- literal, uppercase, mixed-case of met toegelaten separators;
- percentencoding en ASCII `\u`, `\U` of `\x`-escapes;
- HTML numeric/named entities;
- fullwidth-Unicode, ook wanneer die eerst Unicode-escaped was;
- standaard Base64, Base64url en Base32, met of zonder gebruikelijke padding;
- gemengde/nestende combinaties van de verkleinende tekstnormalisaties.

Normalisatie is nooit onbeperkt: maximaal 16 rondes, maximaal 16 MiB input en
maximaal 4096 Base64/Base32-kandidaten. Iedere ronde deelt dezelfde absolute
jobdeadline. Als de normalisatie na de grens nog verandert of de
kandidaatgrens bereikt wordt, faalt de capture gesloten. Een willekeurige
UUID of Base64-string die niet tot de werkelijke token decodeert is geen
false positive. Geen gedetecteerde credentialvorm kan een raw receipt
bereiken. De concrete downloader geeft na die ene scan alleen een interne,
niet-duurzame attestatie door; de orchestrationlaag scant een alternatieve of
testtransportresponse zonder attestatie zelf. Daardoor blijft de
beveiligingsgrens dubbel afgedwongen zonder elke echte body tweemaal te
normaliseren.

## HTTP, body en absolute resourcegrenzen

Eén monotone deadline van 30 seconden wordt aan het begin van de job gemaakt.
Diezelfde deadline geldt voor lokale validatie, wachten op de capturelock,
HTTP-open, alle reads, credentialscan, JSON-parse, structuurscan, raw opslag en
capturecommit. De per-I/O-timeout is maximaal 10 seconden en wordt ingekort
tot de resterende jobtijd.

De body wordt vóór JSON-parse in blokken van maximaal 64 KiB gelezen met:

- maximaal 16 MiB exacte responsebytes;
- maximaal 258 readpogingen;
- exacte `Content-Length`-controle indien aanwezig;
- alleen HTTP 200, de exacte request-URL, identity transportencoding en
  `application/json`.

Een oversized declared length wordt vóór de eerste bodyread geweigerd. Een
stream zonder lengte kan noch de byte- noch de chunkgrens passeren.

`urllib` kan 401/403/429/5xx als `HTTPError` opwerpen vóór er een responseobject
is. Die vormen worden expliciet vertaald naar
`SynopticNativeProviderResponseError(response_class="provider_error")` met
alleen de veilige numerieke status en retryability. Hun body wordt niet
gelezen, gescand, opgeslagen of in fouttekst opgenomen; er ontstaat dus geen
receipt voor bytes die de responsegrenzen niet doorliepen. Responseobjecten
met dezelfde statussen volgen hetzelfde typed pad.

## Exacte opslagvolgorde

De volgorde is fail-closed:

1. maak de absolute deadline en valideer job/payload;
2. bouw de credentialvrije canonieke querycontext;
3. verifieer een bestaande complete capture of eerdere attempts;
4. lees de credential pas als een nieuw request nodig is;
5. voer exact één begrensde HTTP-requestattempt uit;
6. scan de exacte body op credentialecho;
7. schrijf de exacte upstreambytes en querycontext atomisch in
   `RawArtifactStore`;
8. voer pas daarna UTF-8/JSON- en response-inspectie uit;
9. commit alleen een succes/structureel-ongeverifieerde succes-envelope of de
   strikt herkende officiële code-2-ambiguïteit als complete capture.

Ongeldige UTF-8/JSON, dubbele keys, `NaN`/`Infinity`, te grote getallen,
diepte/breedte/nodebudgetoverschrijding en onbekende envelopes blijven exact
als receipt bewaard maar krijgen geen complete capture.

## Providerfouten en niet-vergiftigende retries

Een response die de HTTP- en credentialgrens passeert krijgt een expliciete
`request_attempt` van 1 tot en met 8 in fetch-ID, upstream-ID en provenance.
Daardoor blijven meerdere antwoorden op dezelfde logische job afzonderlijk
en exact aantoonbaar.

De inspector onderscheidt retrybare en deterministische afkeur. 429, 5xx en
expliciete tijdelijke/rate-limitmeldingen mogen naar de volgende genummerde
attempt. De exacte afgekeurde bytes blijven bewaard, maar hun al berekende
classificatie wordt niet opnieuw geparsed. Zo vergiftigt een tijdelijke
providerfout de job niet permanent en veroorzaakt dezelfde oude response geen
herhaalde CPU-piek.

Authenticatie-/toegangsafkeur en JSON-/structuurcontractfouten zijn
deterministisch `non-retryable`. Binnen hetzelfde workerproces wordt die
jobuitkomst als compleet onthouden: een herhaling doet geen netwerkrequest,
rawread of JSON-parse. De cache bevat alleen veilige classificatievelden, geen
payload of credential, is LRU-begrensd tot 128 items en is niet de duurzame
bron; na een procesrestart kan het immutable receipt één keer opnieuw worden
geverifieerd en geclassificeerd.

Timeouts, deadline-expiry en I/O-/transportfouten hebben geen aanvaarde
responsebytes en blijven retrybaar met hetzelfde attemptnummer. Een geldig
receipt na een lokale inspectie-/commitcrash wordt netwerkloos hervat. Voor
een complete capture retry gebruikt de kandidaat één gecombineerde
capture/receipt/bytes-read met exact één SHA-pass en daarna de begrensde
resultaatcache; dezelfde raw JSON wordt niet opnieuw geparsed of structureel
gescand. Gaten in de attemptreeks, gewijzigde querycontext of meer dan acht
attempts falen gesloten.

## Actuele code-2-vormen

Synoptics huidige documentatie toont providerfouten met onder meer
`RESPONSE_CODE: 2`, een `HTTP_STATUS_CODE` (zoals 401 of 429) en soms zonder
`NUMBER_OF_OBJECTS`. Die vorm wordt als `provider_error` behandeld en kan geen
capture, nulresultaat, droogte of dekking verklaren.

Alleen de officiële no-stations/supporttekst wordt als
`ambiguous_no_data_or_access` herkend. `NUMBER_OF_OBJECTS` mag daarbij
ontbreken of exact nul zijn, `STATION` mag ontbreken of leeg zijn, en
`HTTP_STATUS_CODE` mag ontbreken of 200 zijn. De uitkomst blijft onbekend,
omdat dezelfde tekst ook ontoegankelijke stations kan betekenen.

## Eerlijke successtructuur

`RESPONSE_CODE: 1` en een kloppende stationcount bewijzen op zichzelf nog niet
dat de gevraagde neerslag- en QC-structuur aanwezig is. De inspector controleert
daarom voor de sterke structurele klasse alle volgende onderdelen samen:

- een niet-lege, begrensde lijst met unieke `STID`, geldige coördinaten en
  `OBSERVATIONS.date_time`;
- zowel `precip_intervals_set_*` als `precip_accumulated_set_*`, met eindige
  numerieke/null-arrays van exact dezelfde lengte als `date_time`;
- overeenkomstige metadata onder beide categorieën van `SENSOR_VARIABLES`;
- `QC_FLAGGED` voor alle afgeleide neerslagvelden en een begrensde root
  `QC_SUMMARY` met toegepaste tests en een niet-negatieve flagged count;
- units voor zowel `precip_intervals` als `precip_accumulated`.

Alleen dan luidt de status `verified_precip_sensor_qc_structure` en mag het
resultaat de provider-derived intervalsemantiek en structureel bewaarde QC
noemen. Ook dan worden waarden en vlaggen niet meteorologisch geïnterpreteerd.

Een API-succesenvelope die niet aan die volledige vorm voldoet wordt niet als
weerbewijs afgekeurd of ingevuld: hij wordt exact bewaard als
`success_structurally_unverified`. Dan zijn semantic class, evidence origin,
QC-status en neerslagsemantiek expliciet `not_claimed`/`uninterpreted`; er
ontstaat geen impliciete station-, neerslag- of QC-claim.

## Operationele grens

De legacy `synoptic_fetch`-service is niet aangepast. Deze kandidaat heeft
geen servicehandler, schedulerpad, Home Assistant-projectie, deploybestand of
live activatie gekregen. Centrale globale planning/rate-control en een
meteorologisch gecertificeerde decoder blijven verplicht vóór wiring.
