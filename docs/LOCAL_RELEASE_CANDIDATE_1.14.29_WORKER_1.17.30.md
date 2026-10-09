# Lokale releasekandidaat 1.14.29 / worker 1.17.30

Status: reproduceerbaar lokaal gebouwd op 2026-08-30; niet gedeployed.

## Inhoud

- worker-only metrische celtracking voor DWD en Met Office;
- duurzame checksum-gebonden trackingheads;
- dezelfde-track schaduwvensters voor aankomst en vertrek;
- geometrische koppeling van canonieke METAR-, Netatmo- en Synoptic-feiten;
- expliciete tracking-/ETA-/grondmatchdiagnostiek in readiness;
- gecorrigeerde enkele Synology Compose-route met worker-only bootstrap onder
  het persistente NAS-volume en lage resourceprioriteit.

Er is geen operationele passagekans, waarschuwing of ground-confirmation
toegelaten. Alle nieuwe bewegings-, ETA- en grondmatchresultaten blijven
`shadow_candidate_unadmitted`.

## Reproduceerbare artefacten

Twee onafhankelijke lokale builds (`r1` en `r2`) leverden byte-identieke
bestanden:

| bestand | SHA-256 |
| --- | --- |
| `storm_tracker_v3_v1.14.29_deploy.zip` | `10c5bb9deb16a0ce0477ec185d400b9f897801ac9c55ebffbca842f85ee16e31` |
| `storm_tracker_worker_v1.17.30_deploy.tar.gz` | `8957237782dafb1bf14bb38be999e6e8b56e608d019939509d5f31e1fc6bd797` |
| `storm_tracker_release_v1.14.29_worker_v1.17.30_evidence.json` | `f45181fbc526bc943781d97bdbc2a5186995a1e39e7b662981bc42390f196dec` |

De evidenceverifier herlas versie `1.14.29`, worker `1.17.30` en
`deployment_performed=false`.

## Lokaal bewijs

- brede provider-/tracking-/readinessbatch: 286 geslaagd, 1 bewust
  overgeslagen;
- release-, versie-, service- en packagingbatch: 117 geslaagd, 1 bewust
  overgeslagen;
- NAS worker-only packagingbatch: 34 geslaagd;
- verzegelde echte DWD-replay: 36 scans, 72/72 regio-overgangen vooruit,
  0 onbepaald, maximaal 29 bevestigde bewegingsklare tracks;
- vast offline geometrienet: 43 passagekandidaten, waarvan 36 toekomstige
  aankomsten tussen circa 1,9 en 83,9 minuten, zonder forecast- of
  waarschuwingclaim.

## Nog open vóór productie

1. echte opeenvolgende Met Office-replay en gelabelde DWD edgecases;
2. onafhankelijk bewezen fysieke-sitealiases, groundsemantiek en
   sensorcapabilities;
3. held-out passage-evaluatie en kalibratie;
4. KNMI-metrische gridcertificatie en NOAA niet-affiene reprojection;
5. echte DS1618+ build/start, MQTT-, filesystem-, kill-, resource- en
   thermische soak;
6. gecontroleerde canary en rollback, uitsluitend na expliciete toestemming.

Deze kandidaat is een verifieerbare verpakkingsbasis. Hij is nog geen bewijs
dat het product live op de NAS draait of operationeel betrouwbare waarschuwingen
mag geven.
