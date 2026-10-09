# Lokale releasekandidaat 1.14.30 / worker 1.17.31

Status: reproduceerbaar lokaal gebouwd op 2026-08-30; niet gedeployed.

## Inhoud

- alle worker-only functies uit kandidaat 1.14.29 / 1.17.30;
- begrensde, hashgebonden capture/replay van echte opeenvolgende Met Office
  ODIM RATE-scans zonder MQTT- of Home Assistant-publicatie;
- correctie van de echte-data-fout waarbij floating-pointreductie een geldige
  Lucas-Kanade-covariantie ten onrechte als niet-symmetrisch kon afwijzen;
- dezelfde worker- en integratie-engine blijven byte-identiek gespiegeld;
- de Synology Compose-route blijft worker-only, persistent en laag geprioriteerd.

Er is geen operationele passagekans, waarschuwing of ground-confirmation
toegelaten. Alle bewegings-, ETA- en grondmatchresultaten blijven
`shadow_candidate_unadmitted`.

## Reproduceerbare artefacten

Twee onafhankelijke definitieve lokale builds (`r4` en `r5`) leverden
byte-identieke bestanden:

| bestand | SHA-256 |
| --- | --- |
| `storm_tracker_v3_v1.14.30_deploy.zip` | `3fa8eb7704b9a5b19d0e14d8851e005d3d4c43816896b31b91e734cff3fe9433` |
| `storm_tracker_worker_v1.17.31_deploy.tar.gz` | `95175aabbb4778232cf4d167e8f41aec3abcbf11feb7093f683ab99be5e95312` |
| `storm_tracker_release_v1.14.30_worker_v1.17.31_evidence.json` | `e3b8853985eb04493e8d1f505be03016c91dda72590cec9d56ab7d2c1026f8b3` |

De builder en evidenceverifier rapporteren integratie `1.14.30`, worker
`1.17.31` en `deployment_performed=false`. Het canonieke interne
`evidence_sha256`-veld is
`5383850965332fdf406ec5f2cacb94a53355f7a2ee89ec86155f07c0f6fbc361`;
de tabel gebruikt voor alle drie rijen de SHA-256 van de feitelijke
artefactbytes.

## Lokaal bewijs

- worker-only/providerbatch: 414 geslaagd, 1 bewust overgeslagen;
- gerichte Met Office/trackingbatch: 126 geslaagd, 1 bewust overgeslagen;
- numerieke fix en replaycontract: 18 geslaagd;
- aparte bijgewerkte RAW-requirements-v2-poort: 41 geslaagd, 1 bewust
  overgeslagen; de bevroren workerinventaris en bronpins zijn opnieuw aan de
  actuele releasebron gebonden zonder admission- of capaciteitsclaim te
  versoepelen;
- verzegelde echte DWD-replay: 36 scans, 72/72 regio-overgangen vooruit,
  maximaal 29 bevestigde bewegingsklare tracks en 36 toekomstige vaste-
  testpuntaankomsten;
- echte Met Office-replay: acht opeenvolgende scans over 105 minuten en de
  UTC-daggrens, telkens de volledige native grid in 45 tegels, maximaal 463
  bevestigde bewegingsklare trackobjecten, 56 passagekandidaten en 50
  toekomstige vaste-testpuntaankomsten;
- in beide replays bleven forecast, waarschuwing en admission expliciet uit.

De Met Office-sequence-ID is
`482d95893b60e3a3390397d68a6f82c9559fe6412e8e4aa97dad4695571e5df9`.
De bronobjecten en listings staan content-addressed onder de lokale
`.analysis`-evidenceboom en zitten bewust niet in de deployarchieven.

## Nog open vóór productie

1. langere gelabelde DWD- en Met Office-lifecyclegevallen;
2. onafhankelijk bewezen fysieke-sitealiases, groundsemantiek en
   sensorcapabilities;
3. held-out passage-evaluatie en kalibratie;
4. KNMI-metrische gridcertificatie en NOAA niet-affiene reprojection;
5. echte DS1618+ build/start, MQTT-, filesystem-, kill-, resource- en
   thermische soak;
6. gecontroleerde NAS-canary en rollback, uitsluitend na expliciete
   toestemming.

Deze kandidaat is een verifieerbare verpakkingsbasis. Hij is nog geen bewijs
dat het product live op de NAS draait of operationeel betrouwbare waarschuwingen
mag geven.
