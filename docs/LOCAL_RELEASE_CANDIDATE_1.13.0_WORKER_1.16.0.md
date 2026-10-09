# Lokale releasekandidaat: integration 1.13.0 / worker 1.16.0

Status: reproduceerbaar lokaal gebouwd en gevalideerd; niet gedeployed.

Kandidaat-ID: `20260811-i1.13.0-w1.16.0-rc1`

## Besluit

Deze kandidaat is geschikt voor overdracht naar een afzonderlijke
eindvalidatie. Hij is niet automatisch geschikt voor live activatie. Er is geen
Home Assistant-, MQTT-, Zigbee2MQTT-, provider- of storestate gewijzigd en er
is geen deployment uitgevoerd.

```json
{
  "integration_version": "1.13.0",
  "worker_version": "1.16.0",
  "service_wiring_enabled": false,
  "production_enable_ready": false,
  "deployment": false
}
```

## Artefacten

Primary:
`.analysis/release-candidates/20260811-i1.13.0-w1.16.0-rc1`

Onafhankelijke reproductie:
`.analysis/release-candidates/20260811-i1.13.0-w1.16.0-rc1-repro`

| Bestand | Bytes | SHA-256 | Status |
|---|---:|---|---|
| `storm_tracker_v3_v1.13.0_deploy.zip` | 642.824 | `aeb419b772b348605069a383891ed0b0938115f83fd7c44f6cf583d4d58b5e72` | primary/repro byte-identiek |
| `storm_tracker_worker_v1.16.0_deploy.tar.gz` | 1.874.023 | `457cf9ba0f1513c39eab193b91016efa06a8c88ca265fe44f3fc2406d4f46a7e` | primary/repro byte-identiek |
| `storm_tracker_release_v1.13.0_worker_v1.16.0_evidence.json` | 44.299 | `59dd0bfa24a18d79ebb61ad783bc53668ca1e03a89ae64a55ab1207815e1ccac` | primary/repro byte-identiek |

Evidencecontract:
`stv3-local-release-artifact-evidence-v1`

Interne evidencecontracthash:
`7044f2f98220ea7a24cea9655cd4e0bf4d583b237a887b616376e4b6354401b7`

De interne contracthash is niet de filehash; beide zijn afzonderlijk
geverifieerd.

## Sourcetrees en inhoudscontrole

| Tree | Bestanden | Bytes | Contentroot SHA-256 |
|---|---:|---:|---|
| integration | 114 | 2.914.661 | `3e1ce740b384422a6191d9a2a15ba16723e89d0a30249dde4cea77ca77251126` |
| worker | 198 | 10.091.466 | `db3743eb92f50b5eb1a1467d6969ccfd25bdf6fc62eccb8d5904cde62f6e2673` |

De primary- en reprobuild hebben voor ieder artefact dezelfde volledige bytes.
Beide zijn gecontroleerd op cachebestanden, banned bestanden, private keys,
databases, nested archives, reparse points en symlinks; alle zeven categorieën
tellen nul.

## Testbewijs

De finale offline matrix telt 251 testbestanden en 4,949 nodes in 48 seriële
batches met 33m59s getelde batchduur:

- 4,934 passed;
- 15 verklaarde skips;
- 0 failures;
- 0 errors;
- 0 warnings onder `-W error`.

De 15 skips zijn exact 4 `open-SQLite platform`, 6 `Windows symlink`, 1
`HSAF local`, 1 `AEMET live`, 2 `GOES live` en 1 `Met Office live`. De twee
CloudFerro-livecases in
`tests/test_opera_integration.py` zijn volledig uitgesloten en zitten niet in
de node- of skiptelling.

Inventarisregistraties:

- testinventory: `ce7d1eca...bc2818c`;
- nodeinventory: `da3d1d96...64948`.

Zie `ROADMAP_EXECUTION_STATUS_2026-08-11.md` voor scope, bewijs en open gates.

## Reclamation- en filesystemgrens

De echte lokale vijfrollenketen prepareert, commit, applyt, heropent en replayt
lifecycle, execution, scheduler, raw en capacity in de vaste volgorde. Stale
registry-currentness vóór COMMITTED faalt gesloten; abort plus exact successor
is bewezen. V1 withheld-bewijs blijft niet-positief.

Live reclamation blijft geblokkeerd op:

```text
exclusive_filesystem_owner_dirfd_policy_required_for_crash_atomic_hardlink_exclusion
```

Daarboven blijven doel-OS-directorydurability, echte power-loss/disk/media-
faults, resource-/thermal soak en servicecompositie open. De lokaal gebouwde
artefacten wijzigen die grens niet.

## Eindvalidatie vóór iedere deploy

Een latere deploybeslissing vereist minimaal:

1. herhash alle drie artefacten tegen deze tabel;
2. valideer evidencecontract, tree-roots en `deployment_performed=false`;
3. voer installatie- en rollbackdry-run uit op een geïsoleerde testomgeving;
4. bewijs doel-OS filesystemownership en directorybarriers;
5. voer power-loss/disk-/mediafault- en NUC HA/Zigbee2MQTT-soak uit;
6. herbevestig dat service-, production- en science-admissionflags alleen via
   een afzonderlijk beoordeelde wijziging kunnen worden geactiveerd;
7. leg pas daarna een expliciet, apart deploybesluit vast.

Tot die eindvalidatie blijft de juiste handeling: bewaren, hashen en niet
deployen.
