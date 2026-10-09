# Vaste deployafspraak voor Storm Tracker

Bij iedere installatie, update, downgrade of recovery van Storm Tracker moet
`docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md` worden gevolgd. Improviseer geen andere
installatiemethode zolang die procedure bruikbaar is.

Vanaf 30 augustus 2026 wordt Storm Tracker uitsluitend ontwikkeld, getest en
gedeployed op NAS-Home Assistant `192.168.0.15`. De NUC op `192.168.0.144` is
geen deploy- of fallbackdoel meer en mag door deze repository niet worden
gewijzigd of herstart.

De enige vastgelegde SSH-broninstallatie is
`scripts/install_ha_release_pair_ssh.py`, met gebruiker `hassio` en de bestaande
sleutel `C:\Users\drupt\.ssh\codex_nas_ha_deploy` (fingerprint
`SHA256:fmR9WQr3izw9zmvmUtT+wHSOiOGX23P7zTGYx1cxA/w`). Gebruik voor HA-,
integratie-, App- en Supervisorbediening uitsluitend MCP-server
`home_assistant_NAS`. De NAS-SSH-App heeft geen Docker-socket of Supervisor-
token: rechtstreeks `ha store reload` en de historische `docker exec`-route
zijn hier ongeldig. Vernieuw de Store via `supervisor/api`, endpoint
`/store/reload`, methode `post`.

Wanneer de Windows OpenSSH-client op deze NAS-route `Corrupted MAC on input`
meldt, pin dan de door de NAS aangeboden en live bewezen MAC met
`-o MACs=hmac-sha2-256-etm@openssh.com`. Dit is de vaste transportworkaround;
herstart daarvoor geen HA- of SSH-App en maak geen nieuwe sleutel.

Niet-onderhandelbare afspraken:

- voer tijdens iteratieve live ontwikkeling alleen de gerichte regressietests
  uit die de actuele wijziging afdekken, deploy daarna meteen naar NAS-HA en
  verifieer daar live; start geen brede of historische testsuite tenzij de
  gebruiker daar uitdrukkelijk om vraagt;

- de integratie wordt aangeleverd als
  `storm_tracker_v3_v<versie>_deploy.zip`;
- de worker wordt aangeleverd als
  `storm_tracker_worker_v<versie>_deploy.tar.gz`;
- beide bestandsnamen eindigen dus letterlijk op `_deploy.zip` respectievelijk
  `_deploy.tar.gz`;
- de vaste overdrachtsmap op Home Assistant is
  `/backup/storm_tracker_deploy_runs` (buiten de Home Assistant-configbackup);
- gebruik uitsluitend de hierboven vastgelegde NAS-beheerroute; maak niet uit
  eigen beweging een nieuwe SSH-sleutel of alternatieve netwerkroute;
- gebruik voor Supervisorcommando's `ha apps` en de werkelijk ontdekte slug;
  de huidige lokale workerslug is `local_storm_tracker_worker`;
- verifieer archiefnaam, interne structuur, versie en SHA-256 voor installatie;
- maak eerst een rollbackback-up, maar wijzig of verwijder geen Home Assistant-
  of kalibratiedatabase als onderdeel van een normale deploy;
- stop alleen Storm Tracker; HA en Zigbee2MQTT worden niet begrensd, vertraagd
  of gestopt om capaciteit vrij te maken;
- behoud op de uitsluitend voor Storm Tracker bestemde NAS-HA
  `dedicated_nas_fast`, `cpu_limit_percent: 200`, `worker_nice: 0` en
  `cpu_threads: 2`, tenzij de gebruiker dit later bewust wijzigt. De twee
  rekenthreads begrenzen alleen CPU-zware decode/fusie; provider-I/O mag via
  de adaptieve lanes parallel lopen;
- start eerst de worker en wacht op een verse heartbeat plus
  `job_admission_open=true`; schakel pas daarna de integratie in;
- een deploy is pas geslaagd na de volledige nacontrole uit het runbook.

Huidige bewezen NAS-baseline vóór de volgende release:

- Home Assistant Core `2026.8.3`, status `RUNNING`;
- Storm Tracker-integratie `1.14.75`, status `loaded`;
- Storm Tracker Worker `1.17.79`, status `started`, admission open;
- profiel `dedicated_nas_fast`, CPU-limiet `200%`, nice `0`, twee threads;
- dashboard `test-2` gebruikt de werkende OSM-kaartworkaround en toont de
  exacte payloadvrije raw-kalibratie-inventaris; na release `1.14.75 / 1.17.79`
  is live bewezen dat de behouden index verder groeide tot minstens 351
  captures uit 16 bron-ID's en 12 providers, met PostGIS exact 351/351;
- vaste rollbackback-up voor deze paarupdate:
  `storm-tracker-pre-1.14.75-1.17.79` (`c5ab1fb8`).

Kandidaat waarvan de live nacontrole op 5 september is afgekeurd:

- integratie `1.14.76` / worker `1.17.80` in
  `.analysis/release-1.14.76-1.17.80-20260905-a`;
- integratie-SHA-256
  `bcef961f64fd5b5f78a40b4153ffed4ccf8efeaf9c368f610ffdb5947f110c55`;
- worker-SHA-256
  `464ea7c4c6491f3287ab2328821d7a862e017f5d3b6d790e3f3c628601c6086d`;
- content-addressed evidence-SHA-256
  `df5538b6bb00279ba7e707c15391b779041a0ebbf76826c8507e7ea5f9eedbce`;
- De NAS is op 5 september weer bereikbaar, Core `2026.9.0`. De kandidaat
  werd geïnstalleerd maar gaf niet binnen de drie-minutenpoort een nieuwe
  heartbeat. Rollback naar `1.14.75 / 1.17.79` is ingezet. Gebruik de actuele
  herstelstatus in `docs/DEPLOY_2026-09-05_1.14.76_1.17.80.md`; de marker
  `deployment_performed=false` in het lokale build-evidence is geen live status.
- Nieuwe rollbackbackup `0f4843c1`. PostGIS stond uit en is door de gebruiker
  gestart; `192.168.0.21:5433` is weer bereikbaar. Laatst behouden index:
  573 raw, 393 projected, 180 ready. Rollbackpaar is weer online/loaded met
  admission open; eerste volledige tick en outboxdrain nog niet bewezen.
