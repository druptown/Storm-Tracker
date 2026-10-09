# Worker operational-snapshot publication coordinator

Status: kandidaat, volledig **unwired**. Dit document beschrijft
`storm_tracker_worker/operational_snapshot_publication.py`. De module is niet
geregistreerd in de worker service, Home Assistant, MQTT, config, versie of een
deployartefact.

## Doel en grens

De coördinator vormt de laatste worker-side overgang van één gevalideerde
`DesiredState` naar één duurzaam `operational_snapshot_v1`-head en vervolgens
naar een geïnjecteerde transportpublisher. Hij haalt zelf geen data op, voert
geen radar-, raster-, provider- of trackingberekening uit en kent Home
Assistant niet.

Zonder een geïnjecteerde rijke, pure snapshotbuilder bouwt hij voor elk actief
target expliciet `unknown`. Daardoor kan afwezige of mislukte meteorologische
evidence nooit stilzwijgend veranderen in droog, regen, ETA of een ander
positief weerfeit. Een toekomstige rijke builder blijft volledig eigenaar van
de meteorologische semantiek; deze coördinator valideert en transporteert
alleen.

Harde grenzen:

- maximaal 64 targets uit een reeds gevalideerde `DesiredState`;
- maximaal 64 KiB voor de canonieke control envelope;
- één in-flight coroutine per coördinator;
- coalescing bewaart alleen de nieuwste nog niet verwerkte statusaanvraag;
- alleen begrensde tellers en statusvelden gaan de builder in;
- diagnostiek is O(1) en leest geen store, payload, target of coördinaat.

## Eén meteorologische waarheid

`OperationalSnapshotStore` blijft de enige duurzame meteorologische waarheid.
Alleen deze store bevat de canonieke envelope en snapshot. De aparte
`PublicationOutboxStore` is bewust klein en bevat uitsluitend:

- revision;
- SHA-256 van de envelope;
- SHA-256 van de gewenste toestand;
- SHA-256 van de deterministische publication-ID;
- transportstate (`staged`, `pending` of `published`);
- pogingsteller en transporttijdstippen.

De outbox bevat geen envelope, snapshot, target-ID, positie, providerfeit,
raster, observatie of credential. Er ontstaat dus geen tweede bron waarin
meteorologische inhoud kan afwijken. De single-row SQLite-database heeft een
harde main-database page cap van 1 MiB, gecontroleerde `DELETE`-journaling,
een exact gecontroleerd schema/manifest en een canonieke SHA-256 over iedere
volledige transporthead. Daardoor bestaat geen blijvende of door een pinned
reader onbeperkt groeiende WAL-bron; een schoon afgeronde transactie laat geen
`-wal` of rollback-journal achter. Een inhoudelijk geldige maar zonder
bijbehorende checksum gewijzigde state of teller faalt gesloten.

Een transport-outbox is hier noodzakelijk omdat een SQLite-commit en een
externe publish nooit in één atomaire transactie kunnen worden opgenomen.
Zonder duurzame transportstate is na een procescrash niet te onderscheiden of
een committed head nog gepubliceerd moet worden. De gekozen garantie is
at-least-once: verlies van een publishresultaat kan een byte-identieke dubbele
publicatie veroorzaken, maar nooit een overgeslagen nieuwe snapshotrevision of
een gereconstrueerde payload.

## Commit- en publishprotocol

Voor één coalesced aanvraag gebeurt exact dit:

1. herstel eerst eventueel onafgewerkt transport;
2. laad voor iedere werkelijk materialiseerbare coalesced sequence opnieuw de
   actieve, gevalideerde `DesiredState`;
3. lees en valideer de huidige snapshothead;
4. reserveer uitsluitend `head.revision + 1`;
5. bouw een rijke snapshot via de optionele pure builder, of anders de
   target-complete unknown snapshot;
6. valideer de snapshot, eis exact dezelfde unieke target-ID-set als de zojuist
   geladen `DesiredState`, en maak canonieke snapshotbytes;
7. bereken een deterministische commit-ID uit het desired-state-hash en het
   hash van die canonieke snapshotbytes;
8. maak lokaal de canonieke envelope en stage alleen de transportmetadata;
9. commit snapshot + head + idempotencyreceipt via de CAS van
   `OperationalSnapshotStore`;
10. lees de duurzame storehead opnieuw en vergelijk revision, hash én bytes;
11. zet de outbox op `pending`;
12. registreer de publishpoging;
13. lees de duurzame storehead nogmaals;
14. geef uitsluitend `head["canonical_envelope"]` uit die laatste readback aan
    de geïnjecteerde publisher;
15. schrijf pas na publisher-success het transportreceipt `published`.

Precommit-builderbytes worden nooit gepubliceerd. Een forged commitresultaat,
CAS-conflict, rollback, envelopehashverschil of outbox/headverschil stopt de
overgang fail-closed.

## Herstart- en crashmatrix

| Onderbreking | Duurzame toestand | Herstelactie |
| --- | --- | --- |
| vóór outboxcommit | geen outbox, geen snapshot | normale revision opnieuw bouwen |
| na outboxcommit, vóór snapshotcommit | `staged`, snapshothead één revision achter | uitsluitend stage verwijderen |
| na snapshotcommit, vóór pending | `staged` matcht exacte snapshothead | naar `pending`, exacte head publiceren |
| na pendingcommit, vóór publish | `pending` matcht head | exacte head publiceren |
| publisher verwerkte bytes, resultaat ging verloren | `pending` matcht head | dezelfde revision en exact dezelfde bytes opnieuw publiceren |
| na publishedcommit, receiptresultaat ging verloren | `published` matcht head | niets opnieuw publiceren |
| outboxrevision/hash wijkt af | inconsistent | stoppen; niets publiceren of overschrijven |
| snapshothead werd teruggezet/corrupt | storevalidatie faalt | stoppen; niets publiceren |

Een verse coördinator die een bestaande snapshothead zonder outbox vindt,
adopteert alleen de hashes/revision als pending transport en publiceert exact
die head. Hij maakt daarvoor geen nieuwe snapshotrevision.

`async_recover()` stopt na dit exacte herstel. Wanneer een verse coördinator
daarentegen via `async_publish()` tegelijk een actuele expliciete aanvraag
ontvangt, wordt eerst de pending head hersteld en daarna die aanvraag als een
volgende revision gematerialiseerd. De herstelactie mag de nieuwe aanvraag dus
niet als verwerkt markeren zonder inhoudelijke gelijkheidsbewijs.

## Concurrentie en coalescing

Binnen één coördinator bestaat maximaal één drive-task en dus maximaal één
publishercall. Terwijl die call wacht, vervangen nieuwe aanvragen alleen het
in-memory latest-slot. Na afronding worden hoogstens de eerste reeds gestarte
en de allerlaatste coalesced kandidaat committed; alle tussentoestanden worden
niet als nieuwe revisions gematerialiseerd.

Na iedere awaitende publish wordt voor die laatste kandidaat de actieve
`DesiredState` opnieuw geladen. Een HA-wijziging van targetset of authority
tijdens een geblokkeerde eerste publish kan zo niet met verouderde controlstate
in de nieuwste revision terechtkomen.

Over meerdere processen beschermt de single-row outbox de onafgewerkte
revision en beschermt de snapshotstore de head met CAS. Een concurrerende
andere kandidaat kan de staged identity niet vervangen. Bij een werkelijk
CAS-conflict blijft de staged transportmetadata staan en wordt niets
gepubliceerd; recovery mag die alleen verwijderen wanneer de duurzame
snapshothead aantoonbaar exact één revision achter staat.

## Fouten en diagnostiek

Een publisherfout houdt de outbox `pending`; de duurzaam opgeslagen snapshot
blijft de fail-closed unknown of de reeds gevalideerde rijke snapshot. Er is
geen fallback naar een provider, lokale HA-berekening, oude weerclaim of
synthetisch droog feit.

`transport_status()` geeft alleen contract/state, dirty/in-flight, targetcount,
snapshotrevision/hash, transportstate/pogingen, foutklasse/-code en vaste
tellers. De methode decodeert niets en doet geen I/O. Ze bevat geen targets,
coördinaten, displaynamen, providerpayloads of credentials.

## Bewijs in tests

`tests/test_worker_operational_snapshot_publication.py` dekt onder meer:

- unconfigured en target-complete unknown gedrag;
- exacte durable-readbackbytes en commit-vóór-publish;
- deterministische desired-state/snapshot-binding;
- rijke builder zonder semantische herschrijving;
- publisher-resultaatverlies en byte-identieke herstartretry;
- snapshotcommit- en outboxcommitresultaatverlies;
- echte subprocess-crashes vóór en na de SQLite-commitgrens;
- publishreceipt-crashes vóór en na commit;
- adoptie van een bestaande duurzame head;
- mismatch, rollback en expliciete CAS-conflicten;
- single-flight en latest-only coalescing;
- desired-stateherlading over een geblokkeerde publish en expliciete aanvraag
  na verse-process pending recovery;
- fail-closed exacte targetsetbinding voor missing, extra en duplicate targets;
- offline publisher zonder fallback;
- 64-target- en 64-KiB-grenzen;
- secret-shaped en oversize fail-closed gedrag;
- metadata-only outboxschema en O(1)-diagnostiek;
- cancellation met een duurzaam retrybare pending head;
- gecontroleerde DELETE-journaling zonder WAL-sidecar;
- AST-isolatie van service, HA, provider, netwerk, raster, engine en MQTT.

## Nog vereiste go-livegates

Deze kandidaat mag pas worden gewired nadat afzonderlijk is beslist en getest:

1. welke worker lifecycle de coördinator bezit en sluit;
2. welke bestaande MQTT-publicatie exact de geïnjecteerde publisher wordt;
3. waar beide SQLite-paden op de add-on persistent storage staan;
4. hoe de rijke meteorologische snapshotbuilder atomair één evidence-cut leest;
5. hoe broker-ACK/idempotency op dezelfde revision/envelopehash werkt;
6. hoe startup eerst `async_recover()` voltooit vóór nieuwe berekeningen;
7. een add-on power-cut/restartproef op de echte NUC;
8. CPU-, temperatuur- en Z2M/HA-latencybewijs onder live belasting;
9. privacyreview van de uiteindelijke publisher en topicrechten;
10. versie-, release- en rollbackprocedure.

Tot die gates groen zijn blijft de module bewust niet live aangesloten.
