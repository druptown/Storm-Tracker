# Netatmo native capture candidate

Status: lokaal en shadow-only; niet gewired, niet gedeployed en niet voor
operationele beslissingen toegelaten.

## Doel en afbakening

`storm_tracker_worker/netatmo_native.py` is een kandidaat-adapter voor de
Netatmo Weather API-operatie `GET /api/getpublicdata`. De adapter bewaart de
exacte upstream HTTP-responsebytes in `RawArtifactStore` schema 3 voordat JSON
wordt geparsed of een meteorologische betekenis wordt onderzocht.

De adapter maakt uitdrukkelijk geen van deze claims:

- geen regen- of droogclaim;
- geen ground confirmation;
- geen bewijs dat een bounding box volledig door stations is bemeten;
- geen uitspraak over de betekenis, eenheid, interval of kwaliteit van een
  meetwaarde;
- geen samengestelde producttijd voor onafhankelijke station- en moduletijden.

Het resultaat houdt daarom `weather_state` en `precipitation_state` op
`unknown`, zet `wet_claim`, `dry_claim` en `ground_confirmation_eligible` op
`false`, en houdt `operational_admission_eligible` gesloten.

## Exact wire-contract

De jobpayload bestaat exact uit:

- `latitude` en `longitude` van het trackercentrum;
- `radius_km`, begrensd op 1 tot en met 20 km;
- een korte canonieke `query_key`;
- `query_kind`, exact `local` of `roaming`.

Netatmo accepteert een rechthoek en geen geodetische cirkel. De adapter rekent
de gevraagde straal daarom deterministisch om naar `lat_ne`, `lon_ne`,
`lat_sw` en `lon_sw`, bewaart die vier exacte wire-parameters plus
`filter=true`, en noemt de geometrie expliciet
`bounding_box_not_geodesic_circle`. Een box die een pool of de datumgrens zou
kruisen wordt afgekeurd in plaats van stil herschreven.

De OAuth access token komt uitsluitend uit het vluchtige, door de parent
gebrokerde execution-context of uit een expliciete testinjectie. Hij wordt
alleen als `Authorization: Bearer ...` verstuurd. De URL en duurzame
querycontext zijn credentialvrij. Redirects en environment proxies zijn
uitgeschakeld; cookies worden niet verstuurd of bewaard. Alleen een exact
HTTP-200 antwoord van de originele request-URL met `application/json` wordt
als response gelezen.

De bestaande parent-broker valideert access- en refresh-token als begrensde
opaque OAuth-waarden en valideert ook de levensduur voordat enige rotatie
wordt gecommit. De refresh-tokenstate is een regulier, niet-symlinked bestand
met exact twee velden; dubbele keys, een onbekend schema, een ongeldige
credentialfingerprint en een bestandwissel tijdens openen falen gesloten. Een
geroteerde refresh-token wordt via een exclusief aangemaakt mode-0600
tempbestand, file-fsync, atomische replace en Linux directory-fsync duurzaam
gemaakt voordat de vluchtige access-token aan een killable child mag worden
doorgegeven. Providerfoutbody's en credentialwaarden worden nooit in de
exceptiontekst opgenomen.

## Schema-3 bewijs

Een geslaagde capture bevat één required member:

| Rol | Inhoud |
|---|---|
| `response_json` | exacte bytes van de `getpublicdata` HTTP-response |

Het receipt bindt onder andere:

- provider `netatmo`;
- product `netatmo_getpublicdata_json_response`;
- jobgebonden fetch-ID;
- SHA-256 van de credentialvrije querycontext;
- SHA-256 en grootte van de exacte responsebytes;
- receipttijd;
- de begrensde transportpoging waarop de succesvolle response is ontvangen;
- `upstream_native_bytes` en `before_decode`.

De complete capture bindt het receipt als `response_json`. De producttijd
blijft `null`: één response kan onafhankelijke station- en moduletijden
bevatten en die mogen niet zonder productspecificatie tot één tijd worden
samengevoegd. Een exacte retry gebruikt
`load_capture_bundle_bytes`; een crash na receipt maar vóór capture gebruikt
`read_receipt_bytes`. Beide paden verifiëren opnieuw de exacte bytes en doen
geen nieuwe netwerkcall.

## Structurele inspectie zonder weerclaim

Pas na de raw write wordt UTF-8 JSON geparsed. De structurele inspectie:

- weigert dubbele JSON-keys, niet-eindige getallen en onbekende waardetypes;
- begrenst responsebytes, JSON-nodes, diepte, objectmembers, arrays, strings en
  keys;
- begrenst het aantal stations, modules en timestampkeys;
- vereist een providerstation-ID, een geldige `[longitude, latitude]`-vorm en
  een begrensde `measures`-map;
- bewaart station-ID's exact als providerstrings;
- bewaart upstream timestamptokens met hun exacte JSON-type en waarde, onder
  andere `time_server`, station-/module-tijdvelden en de exacte stringkeys uit
  `res`;
- leest of publiceert geen `rain_live`, `rain_60min` of andere meetwaarde als
  meteorologisch feit.

De raw bytes blijven de autoriteit. De afgeleide station- en timestamplijst is
alleen een begrensde structurele index voor een latere gecertificeerde decoder.

## Credential- en cacheveiligheid

Voor opslag vindt een begrensde secret-scan plaats. Die zoekt niet alleen de
letterlijke access token, maar ook URL/percent-, JSON/Unicode-, HTML/NFKC-,
Base64-, URL-safe Base64-, Base32-, hex- en met visuele scheidingstekens
gesplitste vormen. Authgerelateerde responsekeys zoals access/refresh token,
Authorization, cookies, client secret/id en application/API key worden vóór
opslag afgekeurd. De scan wordt aan de orchestration boundary herhaald en ook
op elk receipt/capture-resumepad uitgevoerd.

De token, keys, Authorization-header, cookies en providerfoutbody worden nooit
opgenomen in:

- provenance of querycontext;
- receipt-, capture-, upstream- of lock-ID's;
- validationcache;
- resultaatobjecten;
- foutmeldingen.

Transport- en HTTP-fouten worden detailvrij vertaald. Tijdelijke
transportfouten maken geen receipt en geen duurzaam rejectionverdict. Alleen
een deterministische structurele afkeur ná raw-opslag krijgt een immutable,
checksummed en inhoudsgeadresseerd verdict. Deze cache heeft harde grenzen op
entries, per-bestandsgrootte en totale bytes. Onbekende bestanden, symlinks,
checksumfouten en conflicterende verdicts falen gesloten.

## Belastings- en foutgrenzen

- maximaal 8 MiB exacte responsebytes;
- één absolute monotone jobdeadline van 35 seconden;
- maximaal drie transportpogingen met begrensde exponentiële backoff;
- maximaal 4096 stations, 64 modules per station en 2048 `res`-timestamptokens
  per module;
- procesbrede threadlock plus OS-bestandslocks;
- 64 vaste, gehashte slotlock-shards en twee globale locks, zodat unieke jobs
  geen onbegrensde lockfilegroei veroorzaken;
- raw object-, receipt-, capture-, member-, catalogus- en bytegrenzen komen uit
  de schema-3 store;
- validationcache: maximaal 2048 immutable verdicts, maximaal 2 MiB per
  verdict en maximaal 128 MiB totaal.

## Testbewijs

De gerichte suite `tests/test_worker_netatmo_native_capture.py` en de expliciet
synthetische providercontractfixture
`tests/fixtures/netatmo_getpublicdata_contract.json` bewijzen onder andere:

- raw-before-JSON en byte-exactheid inclusief whitespace;
- complete schema-3 receipt/capture/provenance;
- exacte station-ID's en upstream timestamptokens;
- exact retry en crash-resume zonder netwerk;
- geen wet/dry/coverage/ground-confirmation claim;
- directe, encoded, Unicode-escaped en gesplitste credential-echo-aanvallen;
- auth/key/cookie-fieldafkeur vóór raw-opslag;
- tokenafwezigheid in resultaat, catalogus, objecten en validationcache;
- begrensde retries, deadlines, response/JSON/station/module/timestamplimieten;
- immutable deterministische afkeur en niet-duurzame transportfouten;
- serialisatie door onafhankelijke store-instanties via de OS-lock.

De suite gebruikt geen echte Netatmo-call en geen echte credential.

De aanvullende parent-brokersuite bewijst corrupte/dubbele tokenstate,
ongeldige OAuth-waarden en deadlines, rotatiecrashes, directory-fsync en
credentialvrije HTTP-fouten. Samen met service-, native-capture-,
grounddecoder- en registryregressies waren 134 tests groen onder warnings als
fouten. Dit is nog geen runtimepromotie: de acquisition-child krijgt pas een
access-token nadat de afzonderlijke vluchtige parent→child-authority en zijn
no-persistencebewijs zijn aangesloten.

## Open admissiongates

Er is in deze lokale kandidaat geen afzonderlijk geautoriseerd Netatmo
testcredential of vastgelegde toestemming aangetroffen om een live golden te
capturen. Daarom is bewust geen netwerkcall uitgevoerd. Operationele admission
blijft gesloten tot minimaal:

1. API-gebruik en opslagrecht expliciet zijn bevestigd;
2. een echte, gesaneerde golden exact aan het contract is gepind;
3. Netatmo's productspecificatie voor iedere gebruikte measure en tijd exact
   is vastgelegd;
4. een afzonderlijke decoder station/module/QC/intervalsemantiek aan het raw
   bewijs bindt;
5. dekking, rate limiting en wereldwijde queryplanning wetenschappelijk en
   operationeel zijn gevalideerd;
6. een canonical ground evidence store uitsluitend bewezen stationfacts
   accepteert;
7. shadowreplay, onafhankelijke red-team, belastingstest en cutovergates groen
   zijn.

Tot die tijd blijft de bestaande `netatmo_fetch`-handler ongewijzigd en is
deze kandidaat nergens in registry, service, Home Assistant of deploypakketten
aangesloten.
