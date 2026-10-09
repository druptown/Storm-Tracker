# Open-Meteo usage-, rate- en terms-authority

`storm_tracker_worker.openmeteo_usage_authority` is een duurzame maar inerte
providerbrede sidecar voor de gratis Open-Meteo forecast-API. De module doet
zelf geen netwerkverzoek en is niet verbonden met activation, handlers,
recovery, de terminal router, service, Home Assistant of MQTT.

De sidecar is een verplichte toekomstige voorwaarde voor een fetch, niet een
activatiebesluit. Alleen een gehouden `OpenMeteoUsageLease` kan muteren:

1. `reserve(...)` valideert de echte `AcquisitionQueryAuthority`, haar
   expliciet verwachte SHA en exact haar
   `openmeteo_shadow_query_batch()`; daarna worden alle vier quotavensters in
   een SQLite-transactie belast;
2. `begin_fetch(...)` legt voor dezelfde reservation een duurzame eenmalige
   permit vast;
3. `commit(...)` of `fail(...)` sluit de reservation met compare-and-swap;
4. `load`, `audit`, `diagnostics` en `prune` geven alleen begrensde lokale
   toestand terug.

Een retry van `reserve` met exact dezelfde requestidentiteit is idempotent.
Een retry van `begin_fetch` geeft dezelfde permit terug met
`new_fetch_authorized=false`. Dat veld is normatief: bij `false` mag geen
nieuw netwerkverzoek worden gestart. Een terminale retry is alleen
idempotent als uitkomst, capture-SHA of reason-code en de oorspronkelijke
CAS-state exact gelijk zijn.

## Exacte metering en cadence

De op 2026-08-09 vastgezette pricing-policy heeft ID
`omp1:f6637d575b54a5a08ace781d09fee19905a65c29d435b09bad1b822d865a77ee`
en bron [Open-Meteo pricing](https://open-meteo.com/en/pricing). De formule is

```text
charged_units = ceil(variable_count / 10) * query_count
```

De native request bevat exact 19 variabelen. Daardoor kost iedere locatie
twee interne units en kost een batch van `N` locaties `2*N`; een batch met
64 locaties wordt dus nooit als twee calls geboekt. De reservation, permit en
terminale state binden authority-SHA, canonieke querybatch-SHA,
native-request-SHA, query- en variabeleaantal, pricing-policy-ID,
`charged_units` en de gehashte usage-profile.

De standaard interne limieten zijn bewust lager dan de op de reviewdatum
gepubliceerde free-API-grenzen:

| UTC-venster | Interne units | Gepubliceerde grens |
|---|---:|---:|
| minuut | 30 | 600 |
| uur | 240 | 5.000 |
| dag | 1.000 | 10.000 |
| maand | 20.000 | 300.000 |

Alle vensters worden atomair belast voordat een permit kan bestaan. Een
failure geeft geen units terug. Daardoor past een losse batch met 15 locaties
precies in de minuutlimiet; 16 en 64 locaties falen zonder gedeeltelijke
boeking. Splitsing is alleen veilig met nieuwe, volledig gevalideerde
queryauthorities en reservations; een caller mag niet zelf een querybatch
afsnijden of een SHA vervangen.

Het acquisitioncataloguscontract plant `model.openmeteo.region` iedere 900
seconden: 96 mogelijke slots per UTC-dag. De profile bindt zowel deze cadence
als acquisition-catalogus- en cadence-profile-SHA. Met twee units per locatie
is de duurzame standaardgrens:

```text
floor(1000 / (96 * 2)) = 5 locaties
```

Zes locaties passen dus in een losse minuutbatch, maar zouden bij volledige
cataloguscadence 1.152 units per dag vragen en later terecht fail-closed
raken. `sustainable_locations_at_catalog_cadence=5`, de 900 seconden, de 96
slots en de cadence-profile-SHA staan daarom in profile en diagnostics zodat
een latere activation-preflight dit vooraf kan weigeren. De limiet wordt niet
automatisch verruimd en dit is geen toezegging over provider-metering.

## Duurzaamheid en eigenaarschap

- een OS-lock (`msvcrt` op Windows, `flock` op POSIX) en een proceslokale
  registry laten per databasepad maar een eigenaar toe;
- SQLite gebruikt `DELETE` journaling, `synchronous=FULL`, expliciete
  transacties en fsync/barriers; minute/hour/day/month-boeking is ondeelbaar;
- restart na een crash voor commit ziet geen reservation; restart na commit
  ziet de volledige gehashte reservation en een retry belast niet opnieuw;
- een duurzaam vastgelegde permit kan na een crash niet opnieuw als nieuwe
  permit worden uitgegeven;
- state-, row-, permit-, terminal- en prune-chainhashes, een exact
  schemafingerprint, manifestbinding, aggregatecontrole en beschermende
  triggers laten schema-, veld-, teller- en ongeautoriseerde deletetamper
  fail-closed eindigen;
- disk-full voor commit rolt de hele mutatie terug; een fout na een geslaagde
  commit mag niet tot een tweede charge of fetchmachtiging leiden;
- een teruglopende wall clock blokkeert nieuwe reservations en permits.
  `commit` en `fail` blijven toegestaan op de laatste duurzame logische tijd,
  zodat een begonnen fetch niet permanent open hoeft te blijven.

Een aanvaller die een complete, intern consistente oudere filesystemsnapshot
van database en anchor terugzet, kan niet alleen met lokale SQLite-hashes
worden ontdekt. Een latere operationele deployment moet daarom ook de owner
directory tegen vervanging en rollback beschermen; de sidecar claimt geen
hardwarematige monotoniciteit.

De policy is pas geldig vanaf `2026-08-09T00:00:00Z`. Onder de dagcap kunnen
vanaf dat moment theoretisch maximaal `floor(1000/2)=500` minimale
een-locatiereservations per dag ontstaan. Voor de termsstop 30 dagen later is
de absolute bovengrens daarom 15.000 rows, ook bij meer gewenste-statewissels
dan de 900-seconden-cataloguscadence. De gehashte default profile reserveert
64 MiB catalogusruimte. De gepinde main-databasebegroting is 4 MiB plus 2.048
bytes per mogelijke row, dus 34.914.304 bytes. SQLite mag hoogstens drie kwart
van de fysieke envelope als main database gebruiken; zo blijft ruimte voor het
rollbackjournal. De minimale totale envelope is daardoor 46.552.406 bytes. Een
begrensde test meet echte gehashte een-locatierows tegen de 2.048-bytegrens.
Profiles die hun quota niet binnen hun row- en byte-envelop kunnen dragen
worden al bij startup geweigerd.

## Terms, retentie en privacy

De native attribution-policy blijft de canonieke authority:
`oma1:fcdeef70ad4c1273889c00577db71b669dd711102dbaa51efb831ca0b1a55212`.
De usage-policy voegt geen juridisch oordeel toe. Haar geldige interval begint
op `2026-08-09T00:00:00Z` (`1786233600`); een eerdere clock faalt dicht. De
vaste `review_due_at` is `2026-09-08T00:00:00Z` (`1788825600`). Vanaf dat moment
blokkeren nieuwe reservations en permits fail-closed tot code en policy na
een afzonderlijke review zijn vervangen. De eerste waarneming van die deadline
zet vóór de weigering een gehashte duurzame ledger-latch; een daaropvolgende
clockrollback of restart kan de gate dus niet heropenen. Reeds bestaande reservations mogen
nog terminaliseren en historische records mogen worden gepruned. Er is geen
netwerk- of legal fallback.

Terminale rows worden pas verwijderd nadat ieder eraan gebonden UTC-venster
historisch is en daarna nog 62 dagen zijn verstreken. Pruning is begrensd op
1.000 rows per transactie en vormt een gehashte prune-chain. Als zo'n batch
slechts een deel van een oud gedeeld venster verwijdert, wordt alleen dat
reeds lang verlopen venster atomair naar de overblijvende rows herberekend;
dit kan nooit actueel quotabudget vrijgeven. Onopgeloste
`RESERVED` rows worden niet stil verwijderd. Catalogusgrootte en rowaantal
hebben harde maxima. `secure_delete` beperkt gewone SQLite-pagehergebruik,
maar is geen garantie tegen forensisch herstel uit filesystemback-ups.

De catalogus bewaart pseudonieme `acq:`-jobidentiteit, hashes, tijden, status
en units; niet de querybatch, coordinaten, target-ID of displaynaam.
Diagnostics toont alleen compacte policy/profile-ID's, tellers, venstergebruik
en de duurzame cadencegrens. Een toekomstige HTTPS-fetch moet de expliciet
geautoriseerde coordinaten wel naar Open-Meteo sturen; de privacydisclosure
uit de native attribution-policy blijft voor activatie verplicht.

## Niet geclaimd

- geen activatie of operationele admission;
- geen onbeperkt gebruiks-, licentie- of retentierecht;
- geen garantie dat de provider dezelfde metering blijft gebruiken;
- geen meteorologische modelskill, radarwaarheid of ground truth;
- geen automatische batchsplitsing of terms-update.

Wijziging van pricingregels, free-tiergrenzen, variabelen, acquisitioncadence,
catalogus/cadence-profile, attribution-policy of reviewdatum vereist een
expliciete policy- en testupdate. Tot die tijd blijft iedere drift fail-closed.
