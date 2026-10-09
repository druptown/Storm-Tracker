# PhysicalSite-cataloguscontract

Status: lokaal shadowcontract; niet live gewired of gedeployed.

## Identiteitsregel

Een adaptersnapshot is onbevestigd. De resolver negeert daarom de door de
adapter voorgestelde `physical_site_id` en `independence_group`. Een kandidaat
wordt alleen een fysieke site wanneer zijn exacte, case-genormaliseerde
`(provider_id, station_id)` op het gekozen tijdstip voorkomt als
`VerifiedStationAlias` in een geldige catalogus.

Iedere aliasmapping bevat afzonderlijke `VerificationEvidence`: autoriteit,
bronreferentie, verificatietijd en optioneel de SHA-256 van de gearchiveerde
bron. Ook de sitecoördinaten/tolerantie en de independence group hebben
afzonderlijke evidence. Meerdere provideraliases in één `PhysicalSiteRecord`
vormen dus uitsluitend een site omdat elke mapping dat expliciet en
autoritatief verklaart.

Geografische nabijheid is nooit identiteit. Bij een onbekende alias kan een
nabije site als `proximity_not_identity` in het auditresultaat staan, maar
`physical_site_id` en `independence_group` blijven `None`.

## Gedeeld ICAO/STID

Een exact, gelijktijdig gedeeld station-ID van METAR en Synoptic mag een
conservatieve correlatiegroep opleveren. Dat voorkomt dubbel tellen, maar
merge nooit fysieke sites:

- zonder geverifieerde aliases blijven beide resultaten `unknown_alias`;
- `provisional_correlation_group` is niet bruikbaar als fysieke identiteit;
- twee afzonderlijke geverifieerde sites met hetzelfde ICAO/STID moeten
  dezelfde expliciete independence group hebben;
- alleen twee geverifieerde aliasmappings binnen hetzelfde site-record
  leveren werkelijk één `physical_site_id`.

## Coördinaten en versies

De cataloguscoördinaat is autoritatief; de kandidaatcoördinaat is alleen een
consistentiecontrole. Iedere siteversie heeft een expliciete tolerantie in
meter. Een exacte alias buiten die tolerantie wordt
`coordinate_conflict`; er wordt geen dichtstbijzijnde site gekozen.

De afstand gebruikt een globale grootcirkelberekening met genormaliseerde
lengtegraadverschillen. Daardoor werken antimeridiaan en hoge breedte zonder
een lokale vlakke projectie.

Alias-, capability- en sitegeldigheid gebruiken halfopen intervallen
`[valid_from, valid_until)`. Een verplaatste/vervangen installatie krijgt een
nieuwe deterministische site-ID, een niet-overlappende aliasversie,
`replaces_site_key` en expliciete lifecycle-evidence. Historische resolutie
kiest via `effective_at` de toen geldige versie.

## Determinisme en fail-closed gedrag

Alle inputrecords en resultaten zijn frozen dataclasses met tuples.
Catalogusvolgorde wordt canoniek gesorteerd. Site-ID, independence group,
recordfingerprint, catalogusfingerprint, kandidaatfingerprint, batchfingerprint
en audit-ID zijn content-addressed.

De catalogus wordt geweigerd bij onder meer:

- dubbele site-key;
- overlappende aliasmapping binnen één site;
- dezelfde alias gelijktijdig op meerdere sites;
- tegenstrijdige independence groups bij gedeeld ICAO/STID;
- dubbele autoritatieve identifiers, capabilities of sitingvelden;
- alias/capability buiten de sitegeldigheid;
- onbekende replacement, overlappende siteversies of replacementcyclus.

Resolutie onderscheidt expliciet `resolved`, `unknown_alias`,
`alias_inactive`, `coordinate_conflict`, `catalog_conflict` en
`invalid_candidate`. Alleen `resolved` bevat een bruikbare site-ID en
independence group. Exacte kandidaatduplicaten worden éénmaal geresolved en
met occurrence count geaudit.

`to_ground_fusion_registry(effective_at=...)` exporteert uitsluitend actieve,
geverifieerde mappings naar het bestaande immutable
`PhysicalSiteRegistry`-contract. Die export bewaart nu expliciet:

- de volledige catalogusfingerprint op de registry;
- de contentfingerprint van iedere gebruikte aliasmapping;
- een aparte registryfingerprint over de exacte, canoniek gesorteerde
  registryprojectie.

`GroundMatchAssessment` en `GroundConfirmationDecision` dragen die waarden
door. Een beslissingsset met assessments uit een andere registryversie,
registryfingerprint of catalogusfingerprint wordt fail-closed geweigerd.

## Duurzame beslissingsprovenance

De lokale, nog niet live gewirede `MeteorologicalEvidenceStore` vereist bij
iedere nieuwe groundbeslissing:

- één lowercase SHA-256 van de exacte registryprojectie;
- optioneel de lowercase SHA-256 van de bovenliggende catalogus;
- voor iedere afzonderlijk gewogen groundmatch de lowercase SHA-256 van de
  gebruikte aliasmapping.

Deze waarden worden samen met de oorspronkelijke beslissings- en inputreden
in canonieke JSON-enveloppen opgeslagen. Zij zijn onderdeel van de
content-addressed recordidentiteit, de immutable rowhash en de gehashte
inputset. `trace_forecast()` geeft registry-, catalogus- en
mappingfingerprints weer terug. Oude schema-v3-records blijven leesbaar, maar
worden expliciet als provenance-loos getoond; er wordt geen fingerprint
verzonnen.

Dit is bewust een achterwaarts compatibele shadowuitbreiding binnen schema v3.
De fingerprints zijn duurzaam en integriteitsgedekt, maar nog niet als aparte
SQL-indexkolommen beschikbaar. Operationele wiring mag pas volgen nadat de
autoritatieve catalogusinhoud zelf is gevalideerd.

## Externe en live releasepoorten

Lokaal bewijs kan de inhoud van de externe catalogus niet waar maken. Voor
operationeel gebruik zijn nog vereist:

- versioned, gearchiveerde en gehashte crosswalks van relevante autoriteiten
  voor METAR/ICAO, Synoptic STID, MADIS en doorleveringen;
- gevalideerde locatie-, hoogte-, siting-, capability- en sensorhistoriek;
- expliciete regionale tolerantiepolicies en documentatie van station moves,
  replacements en aliasovergangen;
- periodieke drift- en conflictmonitoring zonder automatische merge;
- duurzame opslag van afzonderlijke cataloguswijzigings- en resolutie-audit
  events, naast de reeds lokaal bewezen beslissingsfingerprints;
- shadowreplay met echte globale records en daarna gecontroleerde live wiring
  in HA/worker.

Tot die metadata en liveproeven bestaan, blijft de module bewust shadow-only.
