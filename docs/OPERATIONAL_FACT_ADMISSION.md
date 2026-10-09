# Fact-specifieke toelating van operationele weersclaims

Status: lokaal uitvoeringscontract; niet live gewired en geen meteorologisch
vrijgavebewijs.

## Waarom deze grens bestaat

Een decoderreceipt, een CellTrack, een grondfusieresultaat of een geslaagde
shadowreplay is nog geen operationele weerswaarheid. De publicatielaag accepteert
daarom geen willekeurige rijke snapshotdictionary meer. Zij accepteert alleen
een `CommittedOperationalFactRevision` die exact is gebonden aan:

- de actuele `DesiredState.state_sha256` en `authority_epoch`;
- de volledige actuele targetset en de exacte targetnamen;
- de door de snapshotstore toegewezen revision;
- een contractgeldige workerstatus die samen met de fact revision wordt
  bevroren en tijdens crashreplay niet stilzwijgend wordt herschreven;
- een begrensd evaluatie- en geldigheidsvenster;
- één checksummed admissionreceipt per concrete niet-`unknown` claim.

Ieder ontbrekend, extra, verlopen, withheld, gemuteerd of anderszins
inconsistent bewijs resulteert in een verse target-complete `unknown`-snapshot.
Een oudere regen-, droogte-, ground- of dreigingsclaim blijft dus niet zichtbaar
omdat een nieuwe wetenschappelijke cyclus faalt.

## Claimsoorten

De gate onderscheidt ten minste revisioncompleteness, regio- en
providerfreshness, huidige regen, huidige droogte, grondstatus,
grondbevestiging, beschikbare/afwezige volgende dreiging, gekalibreerde kans,
verwachte passage, UI-storm en kaartartefact. De receipt bindt de canonieke
SHA-256 van exact de waarde en scope die gepubliceerd wordt.

Een receipt bevat daarnaast de bronreceipts, certificaathashes,
hard-processreceipt, desired-stateautoriteit, besluit- en vervaltijd,
disposition en vaste policy-ID. Alleen `admitted` telt. `withheld` is auditbaar,
maar kan nooit door de publicatiegrens.

## Strengere negatieve claims

`current=dry`, `next_threat=none` en revisioncompleteness vereisen een volledige
scan. Droogte, groundclaims, dreiging, kans en verwachte passage vereisen ook
expliciet onafhankelijk validatiebewijs. Dat veld is geen vrijbrief: de
certificate- en source-receipts moeten later door een toegelaten beslisser uit
de echte replay- en validatiebundels worden gegenereerd. Momenteel bestaat geen
operationeel toegelaten generator voor deze rijke claims.

## Mutatie- en herstartgedrag

De volledige committed revision wordt bij ieder gebruik opnieuw canoniek
gehasht. Een geneste mapping die na constructie werd gewijzigd, wordt daardoor
geweigerd. `OperationalFactRevisionStore` bewaart eerst de exacte canonical
revision onder een pipeline-item, met een begrensde terminale retentietail.
Daarna schrijft de publicatiecoördinator de bestaande snapshotstore. De
factrevision wordt pas `snapshot_committed` nadat de exacte duurzame
snapshothead opnieuw is gelezen en byte/hash-identiek blijkt. Pas vervolgens
mag de MQTT-outbox naar pending/PUBACK. Een crash tussen deze stappen wordt via
de snapshothead gereconcilieerd zonder een nieuwe revision of nieuwe klok te
fabriceren.

## Huidige bewijslimiet

De gate bewijst dat onbewezen feiten niet door de softwaregrens komen. Zij
bewijst niet dat een toegelaten feit meteorologisch juist is. Daarvoor blijven
provider/productsemantiek, decoder- en gridcertificaten, fysieke
site/capabilitycatalogi, held-out gebeurtenissen, skillmetrics, restart/fault-
proeven en de NUC/HA/Zigbee2MQTT-soak verplicht.
