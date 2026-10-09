# Begrensde payloads en tijdkwaliteit

Status: lokale hardening, niet gedeployed.

Deze controles zijn uitsluitend operationele resourcecontracten. Ze kijken
niet naar regenintensiteit, celverplaatsing, snelheid, richting of een ander
meteorologisch getal. Een ongewone maar geldige bui wordt hierdoor dus niet
afgekeurd.

## Compressiecontract

`engine/bounded_payload.py` bestaat als byte-identieke Home
Assistant-/workermirror en heeft geen providerdefaults. De caller moet altijd
expliciet opgeven:

- de maximale gecomprimeerde productgrootte;
- de maximale uitgepakte productgrootte;
- optioneel een productspecifieke maximale expansieverhouding.

De decoder:

- begrenst output tijdens het uitpakken, niet pas erna;
- accepteert exact één gzip- of zlib-stream;
- weigert afgeknotte data, concatenated members en trailing bytes;
- kan sync- en async-streams begrensd verzamelen;
- kent geen meteorologische inhoudsregels.

Dit voorkomt dat een corrupte of kwaadaardige kleine payload eerst onbeperkt
geheugen alloceert. Het is geen veiligheidsrem op weersnelheid.

## Momenteel aangesloten lokale paden

- MQTT workerjob: maximaal 1.000.000 gecomprimeerde transportbytes en
  32.000.000 uitgepakte JSON-bytes.
- Worker/HA-rasterartifact: maximaal 32 MiB gecomprimeerd en 32 MiB
  uitgepakt, met vooraf gecontroleerde bestandsgrootte en SHA-256.
- DWD- en OPERA-workerartifact: maximaal 32 MiB gecomprimeerd en uitgepakt.
- Het volledige upstream OPERA-composiet: maximaal 384 MiB, streaming gelezen
  met controle van `Content-Length`; dit is bewust ruimer dan de lokaal
  gedocumenteerde producten van circa 268 MB en is geen neerslagfilter.
- DWD RV tar-member: maximaal 40 MiB en de echte memberlengte moet exact
  overeenkomen vóór HDF5 wordt geopend.
- H SAF H40B: maximaal 8 MiB gecomprimeerd en 160 MiB uitgepakt.
- Opgeslagen originele kalibratieoverlays: maximaal 64 MiB gecomprimeerd en
  256 MiB uitgepakt per record.
- Wereldwijde METAR-cache: maximaal 16 MiB gecomprimeerd en 64 MiB uitgepakt.
- Alle actieve HTTP-providerpaden lezen chunkgewijs met een expliciete
  productspecifieke bovengrens en weigeren onverwachte transportcompressie.
- Ook de losstaande GeoNames-, World Bank- en METAR-dekkingssurveys hebben
  begrensde downloads en begrensde archief/decompressie-output.

Deze grenzen zijn ruim boven de lokaal aangetroffen METAR-cache
(240.220 bytes gecomprimeerd; 1.001.981 bytes uitgepakt). Providerlimieten
moeten later tijdens de live shadowrun als expliciete observabilitymetrics
worden bewaakt; overschrijding wordt een zichtbaar contract-/capaciteitsfeit,
geen conclusie dat het weer niet bestaat.

## Grondmetingen uit de toekomst, zonder tijd of te oud

Een METAR met `observation_time > received_at` werd vroeger stil verwijderd.
De worker bewaart hem nu afzonderlijk met:

- `time_quality="future_clock_skew"`;
- `usable_for_current_decision=false`;
- qualityflag `observation_from_future`.

Hij staat niet in de actuele `observations`-lijst en kan daarom geen actuele
grondbevestiging of droge conclusie veroorzaken. Hij blijft wel in
`quarantined_observations` zichtbaar voor diagnose en latere
clock-healthregels.

Dezelfde regel geldt voor te oude METAR-reports: ze worden niet meer
weggegooid. Ze blijven met `time_quality="stale"`,
`usable_for_current_decision=false`, hun oorspronkelijke weerfenomenen,
gesigneerde leeftijd en policyfingerprint in quarantainebewijs staan.

Netatmo en Synoptic gebruiken dezelfde providerneutrale beoordeling in hun
lokale workerparser:

- Netatmo bewaart de echte module-/meettijd. Een station zonder bruikbare
  meettijd krijgt `time_quality="timestamp_estimated"` en mag niet stemmen.
- Een stale of toekomstige Netatmo-meting blijft in
  `quarantined_observations`; alleen actuele records tellen mee in
  `station_count` en `observations`.
- Synoptic bewaart complete stale of toekomstige neerslagintervallen in plaats
  van ze stil te laten verdwijnen. Het interval, de QC-context en de gemeten
  hoeveelheid blijven intact, maar `usable_for_current_decision=false`.
- De fail-closed groundadapter zet een expliciet gequarantaineerd
  transportrecord nooit om in een natte of droge stem. Hij houdt wel de
  provider-/stationkandidaat en een controleerbare
  `time_quality_quarantined`-reden bij.

## Providerneutrale tijdkwaliteit

`engine/observation_time_quality.py` bestaat als byte-identieke HA-/workerlaag
zonder verborgen defaults. De caller moet een benoemd, gehasht beleid leveren
voor maximale ouderdom, maximale toekomstige klokafwijking en maximale leeftijd
van klokbewijs. De beoordeling:

- bewaart de gesigneerde observatieleeftijd en transportvertraging;
- bewaart afzonderlijk hoeveel de observatie bij ontvangst en evaluatie in de
  toekomst lag;
- bewaart hoeveel de ontvangsttijd vóór de evaluatieklok lag;
- klemt een negatieve leeftijd nooit stil naar nul;
- laat geen toekomstige observatie- of ontvangsttijd als actueel gelden zonder
  vers onafhankelijk bewijs dat de betrokken klokken gesynchroniseerd zijn;
- classificeert stale, future-clock-skew en receipt-clock-skew afzonderlijk;
- verandert de originele timestamps nooit.

METAR, Netatmo en Synoptic gebruiken deze laag nu rechtstreeks voor actuele,
stale, ontbrekend getimede en toekomstige grondrecords. Providerbrede
radar-/satelliet-clock-healthmetingen en duurzame evidence-wiring vereisen nog
live integratie.

## Nog open

- Iedere providerlimiet moet worden gekoppeld aan productdocumentatie en live
  groottepercentielen.
- Quarantainebewijs moet nog naar de nieuwe raw/evidence-store worden gewired.
- Stale en ongeldig getimede grondrecords zijn in de workerresultaten expliciet
  behouden; duurzame aansluiting op de evidence-store blijft open.
- Geen van deze lokale wijzigingen is live gedeployed.
