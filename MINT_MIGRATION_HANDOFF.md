# Storm Tracker - permanente Mint-migratie

Datum: 2026-09-11

## Actieve werkomgeving

- Project: `/home/ikke/storm-tracker`
- Git: gebruikerslokale Git 2.43.0 via `/home/ikke/.local/bin/git`
- Codex-configuratie: `/home/ikke/.codex/config.toml`
- Codex-taak: `Development`
- Taak-id: `019faa64-241a-7450-879f-fbd1a618b2df`

## Volledige projectkopie

De leesbare, oorspronkelijke Windows-projectboom staat onder:

`/home/ikke/storm-tracker-complete/Storm tracker V3`

Daarin staan onder andere de oorspronkelijke integratiebestanden, worker,
providers, engine, projecties, release- en testversies, zip-releases en de
volledige Git-repository. De actieve repository is daarnaast integraal naar
`Storm-Tracker-git` in deze archiefboom gekopieerd.

## Toegang en configuratie

De actieve projectmap bevat `.codex-ha-access` en `.codex-local`. Deze mappen
zijn alleen leesbaar voor de Linux-gebruiker `ikke`. De Codex-configuratie
bevat de beide Home Assistant MCP-verbindingen en de lokale Node REPL.

## Bekende uitzondering

Windows weigerde terminaltoegang tot twee oude sandboxartefacten:

- `storm_tracker_db_rsa_new` in de oorspronkelijke bovenliggende map;
- `.codex-test-tmp` in de oorspronkelijke repository.

De eerste is een oude RSA-sleutel; de actuele ED25519-database- en
deploysleutels staan wel in de actieve projectmap. De tweede is gegenereerde
test-tempdata en bevat geen unieke broncode of Git-historie.

## Verder werken

Open in de Mint Codex-app de taak `Development`. Nieuwe ontwikkeling gebeurt
uitsluitend vanuit `/home/ikke/storm-tracker`.
