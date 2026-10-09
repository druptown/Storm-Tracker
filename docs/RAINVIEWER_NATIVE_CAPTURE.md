# RainViewer exact-byte shadow capture

Status: **geïsoleerde, geteste kandidaat; niet gewired, niet operationeel en
niet meteorologisch toegelaten**.

Implementatie:
`storm_tracker_worker.rainviewer_native:capture_latest_rainviewer_region`.

## Officieel broncontract

De publieke Weather Maps API publiceert een JSON-manifest op
`https://api.rainviewer.com/public/weather-maps.json`. Volgens de
[officiële Weather Maps-documentatie](https://www.rainviewer.com/api/weather-maps-api.html)
bevat dat manifest ongeveer twee uur aan composietradarframes. De tegel-URL
levert een **composite radar reflectivity visualisation**; maximale zoom is 7
en de beschikbare tegelgroottes zijn 256 en 512 pixels.

Cruciaal voor de tijdsemantiek: RainViewer zegt zelf dat `Frame.time` de
**generation date van de kaart** is. De samengestelde kaart kan beelden van
verschillende radartijden bevatten. Deze waarde mag daarom niet worden
voorgesteld als één uniforme radar-observatietijd.

Dezelfde officiële documentatie biedt een afzonderlijke coverage mask tile.
Die is noodzakelijk om geen echo te kunnen onderscheiden van geen
radardekking. Een transparante radartegel zonder dit masker bewijst nooit
`dry`.

De [publieke API-voorwaarden](https://www.rainviewer.com/api.html) beperken de
gratis API tot persoonlijk, educatief en kleinschalig communitygebruik,
vereisen zichtbare attributie, bieden geen SLA en vragen om caching en
beheerste requestfrequentie. De kandidaat blijft mede daarom fail-closed en
ongewired totdat gebruiksrecht, attributie, belasting en soak expliciet zijn
afgetekend.

## Exacte capture-eenheid

Eén job accepteert exact één regio en bewaart negentien afzonderlijke
upstreamresponses:

1. het exacte manifest vóór de eerste JSON-parse;
2. een volledig 3×3 raster van radar-PNG-tegels rond de tracker;
3. de negen overeenkomstige coverage-PNG-tegels.

De tegelkeuze is bewust:

- Web Mercator zoom 5;
- 256×256;
- officiële kleurset 2, Universal Blue;
- `smooth=0`, dus geen door RainViewer toegevoegde vervaging;
- `snow=0`, zodat geen afzonderlijke sneeuwkleur in de visualisatie wordt
  ingebakken;
- x-coördinaten wikkelen correct rond de datumgrens;
- de nog onondersteunde uiterste poolrand faalt expliciet in plaats van een
  onvolledige 3×3-set te publiceren.

De exacte centrumcoördinaten en `horizon_km` zijn onderdeel van tegelprovenance
en capture-identiteit. De kandidaat berekent bovendien conservatief de kleinste
afstand van de tracker tot de vier randen van de volledige 3×3-unie, inclusief
datumgrenswikkeling en een projectiemarge. Een horizon groter dan deze bewezen
capturedekking faalt vóór netwerkverkeer. Dit bewijst alleen welke tegels zijn
vastgelegd; het coverage mask blijft beslissend voor werkelijke
radarbeschikbaarheid.

Alle negentien rollen hebben een afzonderlijke schema-3-receipt. Alleen als
alle bytes duurzaam bestaan, alle structurele controles slagen en de rollen
compleet zijn, wordt atomair één capturereceipt gepubliceerd.

## Fail-closed controles

Transport:

- vaste HTTPS-hosts en vaste paden;
- geen redirects, proxygebruik, userinfo, querystrings, fragmenten of
  alternatieve poorten;
- HTTP 200, identity transport encoding en verwacht content-type verplicht;
- harde body-, Content-Length-, I/O- en totale tijdslimieten;
- een jobbrede deadline van 60 seconden over alle negentien responses;
- geen credential in jobpayload of provenance.

Manifest:

- strikte UTF-8 en JSON zonder dubbele sleutels of niet-eindige getallen;
- API v2;
- vaste officiële tilehost;
- begrensde `radar.past`-reeks;
- exact `time`/`path`-contract en onderlinge overeenkomst;
- selectie op de hoogste geldige generatietijd, niet op toevallige
  lijstvolgorde.

Een read-only contractcontrole op 2026-08-02 bevestigde dat het actuele
manifest ondoorzichtige twaalftekens-hex-ID's gebruikt, bijvoorbeeld
`/v2/radar/619a3539db0d`, terwijl het gepubliceerde documentatievoorbeeld nog
`/v2/radar/<unix-tijd>` toont. De inspector accepteert uitsluitend deze twee
begrensde, waargenomen v2-vormen. Alleen bij de numerieke vorm moet de suffix
met `Frame.time` overeenkomen; een opaque ID wordt uitsluitend als
bronidentiteit bewaard en nooit tot een tijdstip gepromoveerd.

PNG:

- de exacte response is vóór de eerste PNG-inspectie opgeslagen;
- geldige signature, begrensde chunkstructuur en CRC per chunk;
- precies één IHDR, verplichte IDAT en afsluitende IEND;
- exact 256×256, 8-bit en een alfakanaal;
- onbekende kritieke chunks falen gesloten;
- er worden geen pixels gedecomprimeerd of meteorologisch geïnterpreteerd.

De PNG-inspectie certificeert dus een begrensde, checksum-consistente
containerstructuur. Volledige zlib-, filter-, palette- en pixeldecode hoort bij
een afzonderlijke decodergate en wordt hier uitdrukkelijk niet geclaimd.

Een misvormd manifest of een misvormde tegel blijft als forensische raw
receipt beschikbaar, maar kan nooit een complete capture, dry/rain-state of
operationeel resultaat worden.

## Retry en crashsemantiek

De receiptidentiteit is aan job en rol gebonden. Bij een retry worden reeds
duurzame manifest- en tegelreceipts samen met hun bytes in één authority-pass
per rol geverifieerd en hergebruikt. Alleen
ontbrekende responses worden opnieuw opgehaald. Een partiële poging heeft
`capture_count=0`; ze kan niet als complete weersituatie verschijnen.

Een complete retry gebruikt de schema-3 bundel-API en leest/hash't ieder van de
negentien members exact eenmaal. Daarna leveren duurzame, canonieke
successverdicts de manifest- en PNG-identiteiten zonder JSON/PNG-herparse. Ook
deterministische structurele afkeur is content-SHA- en inspectorversiegebonden
duurzaam; timeouts worden nooit als afkeur gecachet. Verdicts zijn per bestand
en totaal in aantal begrensd en worden onder een proceslock atomair met fsync
gepubliceerd. De 60-seconden monotone deadline omvat nu ook threadlock,
procesoverschrijdende provider-capacity- en jobslotlock, authority-reads,
verdicts, raw-store en
capturecommit.

Omdat het manifest een veranderlijk endpoint is, gebruikt iedere
manifestreceipt een upstream-ID die de exacte response-SHA bindt. De actuele
opaque frame-ID is ook onderdeel van radar-tegel- én capture-identiteit. Zo
kunnen een nieuw manifest of een providercorrectie voor dezelfde generatietijd
niet botsen met, of versmelten in, oudere bytes. Bij resume worden provider,
product, fetch-ID, upstream-ID, content-type en volledige provenance opnieuw
tegen het huidige contract getoetst.

## Meteorologische classificatie

De kandidaat verklaart uitsluitend:

- exacte upstream-visualisatiebytes zijn voor deze regio compleet;
- de gekozen kaartgeneratietijd en tilecoördinaten zijn herleidbaar;
- het coverage mask is mee vastgelegd.

De kandidaat verklaart expliciet **niet**:

- kwantitatieve neerslagintensiteit of accumulatie;
- één uniforme observation time;
- rain rate in mm/u;
- regen, droogte, cellen, motion, ground confirmation of forecast;
- operationele toelating.

De kandidaat bewaart per job precies één frame. Hij publiceert daarom ook
`captured_frame_count=1` en `motion_history_eligible=false`. De voor de roadmap
vereiste tijdreeks van minstens twintig echte frames kan niet uit deze capture
worden afgeleid; de publieke `past`-reeks bevat momenteel bovendien typisch
minder dan twintig frames.

Uitvoer blijft daarom:

- `quantitative_semantics_eligible=false`;
- `operational_admission_eligible=false`;
- `decode_status=not_decoded`;
- `weather_state=unknown`;
- `timestamp_quality_state=composite_frame_generation_time_not_uniform_observation_time`.

## Verificatie

`tests/test_worker_rainviewer_native_capture.py` bevat 53 contract- en
adversariële tests voor onder meer exact-byte-before-parse, complete
manifest/radar/coverage-rollen, partiële retries, herstart zonder netwerk,
opeenvolgende veranderde manifests, volledige receipt-resumevalidatie,
SSRF/redirect/content-type/lengthgrenzen, jobdeadline, regiobinding,
gegarandeerde horizon, manifestdrift, opaque frame-revisies, PNG-CRC en het
verbod op rain/dry/observationclaims. Nieuwe regressies tellen exact negentien
authority-reads na store-reopen, verbieden herparse na success/afkeur, bewijzen
dat timeout niet wordt gecachet, begrenzen JSON-diepte en serialiseren twee
concurrerende identieke jobs tot een upstreamcapture.

Daarnaast is op 2026-08-02 één publieke New York-golden volledig door de
nieuwe kandidaat zelf opgehaald en lokaal in de geïsoleerde analysis-store
vastgelegd:

- frame generation timestamp: `1785631200`;
- live opaque framepad: `/v2/radar/019bad4a13ec`;
- manifest generation timestamp: `1785631231`;
- 19 objecten, 19 receipts en exact 1 complete capture;
- totale upstream-payload: 106.248 bytes;
- exact manifest: 766 bytes, SHA-256
  `5ca78ea517fc2a1bbc238bccc480d02c31237395ad95b46ee4af14b1d11b0b92`;
- capture-receipt-SHA-256:
  `f2ad6ae8544dc9bfc4c229f82f3ba83670860c64f116828fbe07e25340781211`;
- voor de New York-vraag van 350 km: conservatief bewezen
  capturebereik 867,23 km;
- volledige schema-/object-/receipt-/capture-integriteitscontrole:
  `healthy=true`, 0 issues.

Deze golden certificeert de transport- en structuurlaag, niet de
meteorologische interpretatie van de kleuren.

Een onafhankelijke read-only redteamreview vond aanvankelijk een blokkerende
stabiele-manifest-ID en P1-gaten in regiobinding, resumevalidatie,
frame-revisie-identiteit en jobduur. Alle bevindingen zijn daarna gerepareerd,
met afzonderlijke regressietests en een nieuwe live golden. Er blijft geen
bekende P0 in deze geïsoleerde capturelaag; decode, tijdreeks en operationele
toelating blijven bewust open gates.

De kandidaat mag pas worden gewired na een onafhankelijke redteamreview,
publieke golden capture, belastingmeting, providerrecht/attributiecontrole en
een expliciete beslissing dat RainViewer alleen als lagere,
visualisatiegebaseerde aanvullende evidence wordt gebruikt.
