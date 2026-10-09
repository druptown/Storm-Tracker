# PostGIS opslagherstel op NAS met worker 1.18.105

Worker 1.18.105 en integratie 1.14.156 draaien op NAS-HA 192.168.0.15. Vanaf de eerste ontvangst op 7 oktober 2026 om 23:34 Brussels worden toegelaten native weersourcebytes en exact gebruikte radarvelden in de bestaande PostGIS opgeslagen. Twee normale radarproducttijden en gewone grondverversingen zijn exact teruggelezen. Dit kwalificeert de daadwerkelijk gecontroleerde opslagroutes, niet voorspellingsnauwkeurigheid of een complete historische replay.

## Installatie en herstelpad

De herstelde Synology DSM-integratie gaf opnieuw de bestaande storm_admin-beheertoegang. Er zijn geen credentials, importerinstellingen of HA-/VM-herstarts gewijzigd. Alleen Storm Tracker is gestopt en via het vaste runbook bijgewerkt. Migraties 0008 scientific passage forecast, 0009 raw replay archive en 0010 one month weather retention zijn transactioneel geïnstalleerd; bestaande 0008 bleek nog niet toegepast. Geen database of nieuwe infrastructuur is aangemaakt.

De worker startte met verse heartbeat 1791408830.979963, online en admission open, queue/failed 0. Pas daarna is de integratie geladen. Profiel dedicated_nas_fast, CPU 200%, nice 0 en twee rekenthreads blijven behouden. Pre- en post-installatie check_config slaagden. De eerste gewone bronbatch duurde 46.1 seconden en meldde PostGIS current.

Gerichte rollback a56d55f8 is voltooid: 149053440 bytes, HA-configuratie zonder Recorder en worker 1.18.104. Bestaande fullbackup 816aa51b is exact teruggelezen: 1053931520 bytes, Recorder inbegrepen. Bronrollback naar 104 laat de nieuwe tabellen en opnames intact. De eerdere retentiefunctie staat met hash in `postgis-retention-before.json`; verwijder nieuwe weersdata niet bij rollback.

NAS-deployrun: `/backup/storm_tracker_deploy_runs/release_1_14_156_1_18_105_20261007T213048Z`. Installer, Store reload, officiële App update en start zijn uitgevoerd via de voorgeschreven route. MQTT-proeven staan in heartbeat-started.txt en raw-status-first.txt; mqtt_dump.txt is na beide hersteld en vergeleken met de bestaande baseline.

## Artefacten en bronversie

De build gebruikt de exacte live104-bron plus uitsluitend het opslagherstel, vanuit geïsoleerde worktree `/tmp/storm-raw-storage-pIx2yF`, commit ecd2d49. De bestaande dirty hoofdcheckout is niet overschreven. Integratiegedrag blijft ongewijzigd; alleen de paarbuildmarker verwijst naar worker105.

- Workerarchief SHA256: d90472016930721bd4f531e5087dedf634ac067dcddaa3ed28dbdd8c5c7b479c.
- Integratiearchief SHA256: 4e35a43b389e0da2b84a5cf2e59b4800b33c3586e803ac96f2c90ee86da861a7.
- Release-evidence SHA256: 4db413bbd5ec33d58c76407615f08b29924d1b7ffe98fa294ed22de267bff868.
- Geregistreerde werkercode SHA256: fc1505c1b6af3c58695de71e32aa2aec42625d81caf1727cbede60373e1c3962.

Archieven en bewijs staan onder `.analysis/release-1.14.156-1.18.105-20261007`.

## Opslag en retentie

Native bytes worden vóór decode vastgelegd, zonder requestheaders, OAuth-responses, tokens of URL-querywaarden. Identieke bytes delen een payload; iedere echte nieuwe ontvangst behoudt haar eigen tijdstempel. Cachegebruik krijgt geen verzonnen nieuwe ontvangst. Exacte kwantitatieve velden, validmask, confidence en contributor_mask worden verliesloos opgeslagen met CRS, affine rasterpositie, eenheden, producttijd en de werkelijk gebruikte regio. RainViewer-klassen blijven klassen, geen berekende mm/h uit kleuren.

Cyclus, effectieve targets/positie, codeversie en daadwerkelijke voorspellinguitgifte zijn gebonden aan de gebruikte velden. Ook een lege passage-uitgifte blijft bewaard. Teruglezen weigert broninhoud die pas na de betreffende uitgiftetijd ontvangen werd. Opslagfouten, onbekende routes en RAM-/retryverlies blijven expliciete gaten; live weerpublicatie kan doorgaan, maar een ontbrekende RAW-cyclus wordt niet als compleet voorgesteld.

De bestaande writer handhaaft nu een rollende kalendermaand voor weershistoriek, inclusief native input, radarvelden, observaties, stationmetingen en forecast/replayhistoriek. De gewone retentiecontrole is begrensd tot eenmaal per vijf minuten en verwijdert alleen vervallen historie. Stationcatalogus, instellingen, coderegistratie en deploy-/rollbackartefacten zijn referentiegegevens buiten deze weersretentie. Eerste live uitvoering: 23:35:08 Brussels, grens 7 september 23:35:08 Brussels.

## Eerste live bewijzen

`raw-readback-first.json` bewijst de eerste commit: 46 native receipts, vier veldrecords, één complete cyclus zonder gaten en zeven echte uitgiftebindings. Uit PostGIS opgehaalde native bytes van vijftien daadwerkelijk ontvangen providers voldoen aan exacte SHA256 en bytecount: DWD, Eumetsat LI, GOES19 GLM/RRQPE, H SAF, KMI APPV4/AWS, KNMI, METAR, Meteofrance, Netatmo, NOAA ERR, OpenMeteo, OPERA en RainViewer. Dit kwalificeert de gelezen captures, niet ieder mogelijk endpoint of elke provider ter wereld.

De volgende read-only audit telt 68 captures, tien veldrecords, dertien cycli en 101 uitgiftebindings met bestaande immutable forecastissues. Beide gebruikte fusievelden zijn 340x340 en lezen met hun oorspronkelijke inhoudsfingerprint exact terug, inclusief validmask, confidence en contributor_mask. Native KNMI-, DWD- en Meteofrance-arrays en visuele RainViewer-klassen lezen eveneens exact terug. De gebruikte producttijd was in deze audit nog 23:30; twee onafhankelijke normale producttijden en een gewone grondverversing blijven vereist voor finale acceptatie.

Home houdt zijn rastercentrum 51.0498071,4.4186241; Groningen zijn eigen 53.2194,6.5665. Alle beschikbare persoonlijke targets blijven in hun eigen vastgelegde Belgische regio. De fictieve tracker is niet verplaatst. Een Home-trackingweigering bij 270 componenten op het product van 23:30 wordt niet opgelost met dit opslagherstel; juist ook dat gebruikte veld en de lege uitgiften blijven beschikbaar voor later onderzoek.

De aparte MQTT-status is `storm_tracker/v1/operational/raw_archive_status`. Eerste dump: current, queuebytes/receipts/cycli 0, dropped_cycles 0. Het cumulatieve dropped_count is 16 en excluded_count 8; dit is niet gelijk aan nul historische opslaggaten. De uiteindelijke read-back bewaart de concrete gatverdeling hieronder, ook wanneer de nieuwste cyclus weer compleet is.

## Acceptatie op gewone broncadans

`raw-readback-qualified-final.json` kwalificeert gewone radarproducten 23:30 en 23:35 voor beide afzonderlijke regio's. Alle 128 unieke native payloads achter 149 echte receipts zijn onafhankelijk uit PostGIS gelezen en op SHA256 en bytecount gecontroleerd, inclusief RainViewer-PNG-tiles en bronlistings. De nieuwste gebruikte fusievelden, native inputs en visuele klassen lezen verliesloos terug; actuele veldfingerprints matchen de productie-identiteit. Er zijn twintig veldrecords, 26 cycli en 205 echte forecastissues met 205 veldbindings. De 23:39 en 23:45 Netatmo-ontvangsten volgen op 23:34; ook KMI AWS en METAR zijn op gewone tienminutencadans opnieuw ontvangen om 23:44. Canonical telling groeit naar 120261 observaties en 75512 stationmetingen.

Van 26 cycli zijn vier expliciet onvolledig: één cyclus bevat mqtt_region_context_missing en cache_origin_missing, drie bevatten http_status_500 en provider_input_not_received. De code noemt hiermee een niet-beschikbare regio-/cachecontext en een bronresponsefout; deze audit bewijst niet welke provider de HTTP500 veroorzaakt en verzint geen oorzaak. Niet-ontvangen input of invoer zonder aantoonbare oorspronkelijke context kan niet worden gekwalificeerd als complete replay. De twee laatste gecontroleerde cycli zijn compleet zonder gaten; alle geregistreerde cycli gebruiken de geverifieerde codehash. Regionale Blitzortung-receipts blijven niet live gekwalificeerd, los van de wel exact gecontroleerde Eumetsat-/GOES-blikseminvoer.

Opslagherstel is hiermee live gekwalificeerd voor daadwerkelijk ontvangen en gecontroleerde routes. Bronuitval, onbekende context en geografisch/API-geblokkeerde routes blijven expliciete uitzonderingen; geen alle-providers-compleetclaim. Broncadans, trackers en meteorologische drempels zijn onveranderd.

## Gerichte controles en beperkingen

68 actuele gerichte capture/HTTP/native/codec/PostGIS/bindingtests slagen in 6.86 seconden. Daarvóór zijn rechtstreeks geraakte writer/provider/MQTT-compatibiliteit, retry/overflow/shutdown en releasebuildercontroles uitgevoerd; geen brede of historische suite. Een echte PostGIS-migratieproef bewees idempotentie, byte-exacte twee receipts, big-endian NaN/mask-rondgang, uitsluiting van toekomstig ontvangen input en maandretentie, geheel teruggerold vóór installatie.

Oud ontbrekend native materiaal wordt niet door deze release teruggevonden. Geografisch niet-toepasselijke bronnen, niet-gekwalificeerde regionale MQTT-invoer en KMI RADQPE/Wunderground zonder key blijven afzonderlijke uitzonderingen. Geen volledige replaysimulatie, regenzonegroepering, 200km-algoritme of 90% gekalibreerde nauwkeurigheid is met dit opslagherstel geïmplementeerd. De oude Belgische monitoring blijft gestopt; uitsluitend de later opnieuw toegestane Groningen-eerste-regenopvolging blijft beperkt actief.
