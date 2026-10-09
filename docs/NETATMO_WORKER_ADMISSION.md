# Netatmo worker-admission en live veldbewijs

Status: lokaal gerepareerd en met contracttests afgedekt; niet live gedeployed.

## Veldwaarneming van 30 juli 2026

De gebruiker meldde op de thuislocatie omstreeks 14:39:50 CEST zeer lichte
regen. Het door de gebruiker aangeduide publieke Netatmo-station
`70:ee:50:29:59:56` toonde rond 14:48:52:

- 0,10 mm in het laatste uur;
- 0,10 mm vandaag;
- een ongeveer één minuut oude stationsmeting.

Een hercontrole rond 14:51:59 gaf dezelfde hoeveelheid en een ongeveer vier
minuten oude meting. Dit begrenst de positieve stationsmeting grofweg tot
14:47–14:48, maar bewijst zonder eerdere droge stationssnapshot niet het exacte
beginmoment.

Rond 15:12:36 stond hetzelfde station op 0,51 mm voor het laatste uur en
vandaag: een toename van 0,41 mm. STV3 bleef tegelijkertijd
`nadert_regio` melden met nul ontvangen Netatmo-stations, terwijl
`netatmo_fetch` nog altijd nul jobs had voltooid of gefaald. Door deze
daadwerkelijke toename is de discrepantie niet meer verklaarbaar als alleen een
oude cumulatieve waarde van vóór de gemelde passage.

Home Assistant meldde gelijktijdig:

- `netatmo_local_stations = 0`;
- `netatmo_local_raining_stations = 0`;
- `netatmo_local_status = onvoldoende_data`;
- workerproces `netatmo_fetch`: gezond/inactief, nul voltooide en nul mislukte
  jobs sinds workerstart.

Dit was geen droge tegenwaarneming. Het publieke station had regen geregistreerd,
terwijl STV3 geen Netatmo-workerjob had toegelaten.

## Oorzaak

Netatmo OAuth is bewust naar de externe worker verplaatst. Secrets mogen niet
meer via MQTT of de HA-runtime worden vervoerd. `_poll_netatmo()` liet een
workerjob echter alleen toe wanneer voor dezelfde regio nog een legacy
`NetatmoProvider` in HA bestond:

```text
worker fetch toegelaten ⇐ engine_id aanwezig in legacy providermap
legacy provider aanwezig ⇐ OAuth-tokenmanager in HA aanwezig
```

Bij de bedoelde veilige configuratie — secrets uitsluitend in de
workeromgeving — bleef die legacy providermap leeg. Daardoor werd geen job
verstuurd en kon ook geen expliciete credential- of providerfout ontstaan.
De status bleef misleidend stil op nul ontvangen stations.

## Lokaal contract na reparatie

Voor iedere actieve `RegionEngine` wordt de begrensde `netatmo_fetch` nu
onafhankelijk van de legacy HA-providermap aangevraagd.

- De worker blijft de enige OAuth-eigenaar.
- MQTT bevat alleen `credential_ref = worker_environment`; nooit secrets.
- De bestaande cache begrenst dezelfde regionale query tot eenmaal per vijf
  minuten.
- Tijdens een refresh mag maximaal vijftien minuten oude, eerder geldige
  Netatmo-data behouden blijven.
- Zonder geldige cache blijft de grondstatus `onvoldoende_data`; nul ontvangen
  stations wordt niet als droge grondwaarheid gebruikt.
- Ontbrekende workercredentials of een providerfout worden voortaan ten minste
  als echte workerjobfout zichtbaar in plaats van vóór jobadmission stil te
  verdwijnen.

Legacy HA-providerobjecten blijven voorlopig bestaan voor backward-compatible
drukstate, maar hebben geen beslissingsrecht meer over workerjobadmission.

## Lokale verificatie

De regressietests bewijzen expliciet dat:

- `_cached_worker_netatmo()` voor iedere actieve regio wordt aangeroepen;
- `engine_id in providers` niet meer als admissionvoorwaarde voorkomt;
- Netatmo-secrets niet in de MQTT-jobpayload staan;
- de overige regionale ground- en providercontracten intact blijven.

Uitgevoerd:

```text
gerichte admission-/pipelinecontracten: 35 passed
uitgebreide Netatmo-, groundadapter- en workerregressie: 88 passed
python bytecode compilation: geslaagd
git diff --check: geslaagd
```

## Nog vereiste live releasegate

Er is bewust niets gedeployed of herstart. Na een later expliciet toegestane
deploy moet read-only worden bewezen dat:

1. `netatmo_fetch.completed_jobs` boven nul komt;
2. een fout ontbrekende credentials expliciet zichtbaar wordt, of de query
   bruikbare stations teruggeeft;
3. station `70:ee:50:29:59:56` bij actuele beschikbaarheid in de regionale
   observaties verschijnt;
4. de lokale grondstatus bij een positieve, verse meting niet langer
   `onvoldoende_data` blijft;
5. polling en OAuth-tokenrotatie gedurende minstens één uur stabiel blijven.
