# Kwantitatieve decoderkandidaten: activatiegrenzen

Status: lokaal/shadow-only. Geen provider uit dit document is hierdoor
operationeel toegelaten, gepland, gedeployed of in Home Assistant geladen.

## Waarom deze grens bestaat

Een exact bewaard providerbestand en een technisch correcte decode bewijzen
nog niet samen dat een raster operationeel als meteorologische waarheid mag
worden gebruikt. De keten moet afzonderlijk aantonen dat productidentiteit,
quantity, unit, tijd, grid, nodata, kwaliteit, provenance en resourcegedrag
allemaal kloppen. Daarom schrijft `CanonicalEvidenceStore` uitsluitend een
`non_operational_candidate_artifact` en bestaat er nog geen positieve
operationele admissionreceipt.

De repositorybrede regressie
`tests/test_worker_quantitative_candidate_isolation.py` controleert dat de
DWD-, KNMI-, NOAA- en toekomstige OPERA-decoder:

- geen productiehandler is;
- door geen ander worker- of Home Assistant-module wordt geïmporteerd;
- geen operationele providerpolicy heeft;
- niet als kopie in de Home Assistant-integratie aanwezig is.

Een providergerichte test alleen is daarvoor onvoldoende: een latere import
in een scheduler, service of helper zou anders buiten die ene test kunnen
vallen.

## Verplichte gates vóór enige positieve admission

Iedere provider moet alle volgende bewijzen afzonderlijk groen hebben:

1. exacte originele upstreambytes zijn vóór parsing als complete schema-3
   capture opgeslagen en bij iedere bewijsstap cryptografisch herleidbaar;
2. productidentiteit, tijdssemantiek, native grid, quantity, unit, gain,
   offset, nodata, undetect en qualityvelden zijn productspecifiek en
   fail-closed gevalideerd;
3. het afgeleide artifact wordt door een onafhankelijke reader volledig
   herlezen en semantisch tegen raw capture plus decoderreceipt gebonden;
4. een aparte beheerde certificeringsregistry bindt exact provider, product,
   decoderidentiteit, versie, configuratiehash en artifactschema;
5. een aparte immutable meteorologische admissionreceipt bestaat; een
   `decoded_complete`-receipt alleen is nooit voldoende;
6. ontbrekende, onvolledige, stale, slechte of onbekende data blijft
   `unknown` en wordt nooit stil `dry`, nulneerslag of afwezigheid;
7. de productiejob draait uitsluitend in het killbare workerchild met absolute
   parentdeadline, correct effectcontract en gemeten maximale CPU/RSS;
8. maximale-regio replay, crash-/disk-full-/timeoutmatrix en echte NUC
   thermische soak zijn groen zonder HA- of Zigbee2MQTT-latentie;
9. event-held-out meteorologische validatie en vergelijking met onafhankelijke
   grondwaarnemingen zijn groen voor meerdere locaties en weertypes;
10. shadow- en canaryfase zijn zonder waarschuwingseffect doorlopen en hebben
    een geteste rollback.

Tot al deze gates groen zijn blijft de operationele admission voor iedere
kwantitatieve kandidaat hard `false`, ook als focused decoder- of
goldentests slagen.
