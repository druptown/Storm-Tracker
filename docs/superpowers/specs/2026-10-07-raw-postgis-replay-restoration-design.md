# Herstel ruwe bronopslag en radarreplay in PostGIS

Datum: 7 oktober 2026. Status: herstel gevraagd; schriftelijke uitwerking wacht
op gebruikersreview. Nog geen productcode gewijzigd of gedeployed.

## Doel en afbakening

Herstel de afspraak dat alle ontvangen meteorologische invoer duurzaam wordt
bewaard, zodat de volledige regenpassage later opnieuw kan worden berekend.
Dit geldt voor alle werkelijk opgehaalde toepasselijke bronnen, niet alleen
radar. Geen extra polling, andere locatie, nieuwe infrastructuur of herbouw
van de oude RAW/receipt/authorityketen. Bestaande PostGIS is de enige duurzame
meteorologische opslag. Historische gaten mogen niet als compleet worden
gepresenteerd. Geen productcode voor het afzonderlijke regenepisodeontwerp.

## Vastgestelde oorzaak

Het ontwerp van 20 september sluit het permanente lokale RAW-archief uit.
Het actieve dunne pad maakt een AioHttpFetcher zonder opslagkoppeling;
providerdecoders krijgen bytes en bewaren resultaten hoofdzakelijk in geheugen.
ThinPostGISWriter bewaart genormaliseerde observaties, fusiesamenvattingen,
tracks en forecastissues, niet de bronbytes of volledige gebruikte QPE-arrays.
De oudere raw_captures-tabel is expliciet payloadvrij en verwijst naar de oude
artifactstore; alleen die tabel weer vullen zou het probleem niet oplossen.

## Gekozen aansluiting

Voeg aan het bestaande bron- en PostGIS-pad een kleine archieffunctie toe:
native ontvangen broninhoud voor herdecodering, plus de exacte daadwerkelijk
gebruikte regionale radarvelden voor directe algoritmereplay. De andere
opties vallen af: alleen contouren verliezen lichte regen/pixels; alleen het
oude lokale archief herstellen brengt de uitgesloten tweede opslag terug.

### Broninvoer

Per provideraanvraag wordt ontvangst vastgelegd voordat de meteorologische
decoder de inhoud verandert of op targets uitsnijdt. Een opname bevat de
ongewijzigde responsebytes, provider/productidentiteit, werkelijke ontvangsttijd,
aanvraagcontext met target-/regiorevisies en decoder-/codeversie. Producttijd
wordt alleen opgeslagen als deze uit echte bronmetadata vastgesteld kan worden;
onbekende producttijd blijft null. Decoderafwijzingen houden ontvangen geschikte
broninhoud beschikbaar, met afwijzingsreden. Foutstatussen krijgen een veilige
statusregistratie, geen willekeurige login- of foutpagina als weerdata.

HTTP-opnames gelden voor JSON, raster-/archiefbestanden en kaarttegels. Leg
context per provideraanvraag vast, niet in één gedeelde mutable current-provider
variabele. GET/JSON-wrappers mogen dezelfde response niet tweemaal opnemen.
Voor de bestaande synchrone satellietfetchers is een aansluiting vóór decode
nodig; alleen hun reeds gefilterde observaties opslaan is geen native RAW.
Blitzortung krijgt dezelfde opname vóór verwerking van ontvangen regionale
berichten, inclusief ontvangen topic/context. Geen nieuwe wereldwijde feed.

Authenticatie-endpoints, OAuth-responses, tokens, cookies, requestheaders en
wachtwoorden worden nooit gearchiveerd. Bronadressen bevatten geen userinfo,
querytokens of geheime padsegmenten. Bewaar alleen expliciet toegelaten
meteorologische aanvraagparameters; geen generieke dump van adapterconfiguratie.
Een onbekende responsevorm mag geen onbeoordeelde credentialpayload opslaan.
Uitsluiting wordt expliciet verantwoord en is geen ontbrekende weeropname.

### Exacte radarvelden

Bewaar elke nieuwe gebruikte native/gefuseerde regionale frame-identiteit met
volledige arrays in het gebruikte regionale grid, maskers, vorm/dtype, CRS,
affine transform, quantity/units, product- en ontvangsttijd, bronbijdragen,
fusie-/decode-/codeversies en regio-revisie. Reflectiviteit en visuele klassen
blijven hun eigen quantity; geen mm/u uit kleuren. Een volledig continentaal
native bronbestand en een regionaal gebruikt veld zijn verschillende objecten.
Bewaar daarnaast de effectieve niet-geheime rekenpolicy en digest/verwijzing
naar de reeds bewaarde exacte deployarchieven; alleen een versienummer of een
fingerprint zonder beschikbare inhoud is geen reproduceerbare uitvoering.

Serializeer arrays verliesloos, zonder pickle, inclusief invalid masks en
beschikbare coverage-/qualitymaskers. Herlezen moet dezelfde waarden, maskers
en georeferentie opleveren. Koppel elke operationele cyclus en uitgegeven
forecastissue expliciet aan de gebruikte frame-identiteiten. Dezelfde bytes
worden niet telkens opnieuw weggeschreven, maar nieuwe ontvangst- en
gebruikscontext blijft afzonderlijk bewaard.

### Database en beschikbaarheid

Maak alleen additieve versiegebonden tabellen in dezelfde PostGIS-database:
payloadbytes met digest/deduplicatie, bronontvangsten met metadata, radarvelden
en cyclus-/issuebindings. Geen wijziging of verwijdering van oude tabellen.
Digest is integriteitscontrole, geen vervanging voor bytes. Bestaande
genormaliseerde metingen, targethistoriek en immutable forecastissues blijven.

DB-I/O en arrayserialisatie draaien buiten de MQTT-eventloop via bestaande
begrensde uitvoering. Geen nieuwe threadschaal, geen extra bronfetch om een
opslagretry te doen. Opslagfouten mogen bruikbare live weerpublicatie niet
stoppen. Hergebruik begrensde retry: bij vol buffer of verlopen vluchtige
invoer registreer expliciet een replaygat en dropped_count; nooit current/
complete claimen. Zonder tweede duurzame spool is perfecte verliesloosheid
tijdens langdurige DB-uitval niet te garanderen. Rapporteer deze beperking.

Geen automatische RAW-verwijdering of impliciete nieuwe retentieperiode.
Huidige retentionfunctie mag nieuwe archieftabellen niet meenemen. Maak bytes,
aantal producten, achterstand en gaten zichtbaar via een apart versioned
archiefrapport op `storm_tracker/v1/operational/raw_archive_status` en veilige
logsamenvattingen. Bestaande exact-key snapshot-/diagnostiekcontracten niet
stil uitbreiden. Gecombineerde opslagstatus wordt alleen current als ook de
ruwe invoer van de betreffende cyclus bevestigd is; alleen de bestaande
observatie-insert is daarvoor niet voldoende.
Meet live groei voordat een toekomstige bewaartermijn/capaciteitskeuze wordt
voorgelegd; geen capaciteit of opslagruimte verzinnen. Bij onvoldoende ruimte
duidelijk degraded/gap, geen oude bestanden of databasegegevens opruimen.

## Replaycontract

Reconstructie gebruikt alleen invoer die op de gesimuleerde uitgiftetijd al
ontvangen was. Begin-/eindtijden van de radar zijn geen exacte grondwaarheid.
Bewaar de oorspronkelijke uitgiften en menselijk gemelde onset apart, en tel
ontbrekende waarschuwingen mee. De passage van 7 oktober is een developmentcase;
afgestelde foutmarges moeten daarna op ongebruikte passages worden beoordeeld.
Een end-to-endreplayrunner en algoritmekalibratie zijn vervolgstappen, niet
onderdeel van deze opslagfix.

Historische bronproducten mogen later alleen worden teruggehaald via bestaande
toegestane toegang. Bewaar oorspronkelijke producttijd en nieuwe ontvangsttijd:
historisch achteraf opgehaald is niet toen live ontvangen. Geen geforceerde
reeks opvragingen, sleutelomzeiling of succesclaim voordat herlezen bewezen is.

## Gerichte acceptatie en deploy

Eerst falende relevante regressies, daarna alleen de opslagaansluiting:

- Native response blijft byte-exact herleesbaar, ook na decoderafwijzing.
- Parallelle aanvragen behouden eigen provider-/target-/regiocontext.
- JSON-wrappers, retries en cacheticks maken geen dubbele payloads of
  vervalste eerste ontvangsttijd; gewijzigde broninhoud blijft afzonderlijk.
- Radararrays/maskers/grid/quantity roundtrip exact; lichte achtergrondregen
  wordt niet door celcontouren vervangen.
- Auth/tokenresponse en geheime headers/parameters komen niet in opslag/logs.
- Alle actuele adapterroutes worden geïnventariseerd; HTTP, eigen synchrone
  fetchers en regionale bliksemfeed kunnen niet ongemerkt buiten capture vallen.
- DB-uitval, bufferoverflow en ontbrekende bronopname worden degraded/gap,
  zonder MQTT/provideruitval te veroorzaken of de pollcadans te veranderen.
- Additieve migratie behoudt bestaande data en laat oude retentie ongemoeid.
- Cyclus-/forecastbindings wijzen naar de daadwerkelijk gebruikte frames;
  herstelbare invoer moet op ontvangsttijd causaal selecteerbaar zijn.

Deploy uitsluitend NAS-HA volgens HA_DEPLOY_EN_HERSTELPROCEDURE.md, met rollback
en geen Core/VM-herstart. Test daarna minimaal twee normale unieke Belgische
radarproducttijden, een gewone grondproviderverversing en een ondersteunde
bliksem-/satellietontvangst wanneer toepasselijk. Lees bytes en velden echt
terug uit PostGIS, controleer integriteit/semantiek/bindingen en scheiding van
targets. Een provider zonder toegang of geografische dekking blijft expliciet
ongekwalificeerd; alleen een groen label of rijtelling is geen replaybewijs.

## Niet inbegrepen

Geen tweede database, objectstore, lokale RAW-spool, corpusstartaudit, nieuwe
credentials, importerwijziging, persoonlijke targetverplaatsing, globale-vector-
productiealgoritme, nieuwe kwaliteitspoort of beloofde 90%-betrouwbaarheid.
