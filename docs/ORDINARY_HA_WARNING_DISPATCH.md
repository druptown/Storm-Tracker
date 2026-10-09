# Eigen waarschuwingmeldingen in normale Home Assistant

## Doel en grens

De expliciete normale-Home-Assistant-runtime (`runtime_mode=legacy`) kan een
berekende naderende passage naar één bestaande Home Assistant-service
`notify.<service>` sturen. Deze functie is standaard uitgeschakeld en wordt
alleen actief wanneer beide waarden expliciet in de legacy-config-entry staan:

```text
warning_notifications_enabled: true
notify_service: notify.mobile_app_voorbeeld
```

Een Storm Tracker-melding is uitsluitend een eigen lokale modelverwachting.
Ze is geen officiële waarschuwing van KMI, KNMI, DWD, AEMET of een andere
meteorologische autoriteit en wordt ook technisch met
`official_warning=false` gelabeld.

Worker-only toont deze velden niet en bezit deze dispatchroute niet. De lokale
opt-in is de dispatch-authority; een worker- of RAW-admissionflag mag een
expliciete normale-HA-runtime niet stil blokkeren.

## Conservatieve toelatingspoort

Een melding wordt alleen verzonden wanneer dezelfde immutable snapshot aan al
de volgende voorwaarden voldoet:

- de lokale StormManager-admission staat open;
- target en radardekking zijn actueel beschikbaar;
- de geselecteerde stormdata is maximaal twintig minuten oud;
- `status` is exact `naderend`;
- `warning_eligible` en `forecast_available` zijn echte booleans `true`;
- ETA is betrouwbaar, eindig en ligt tussen nul en negentig minuten;
- er is geen `forecast_block_reason`;
- de passageclassificatie is `raak` of `rand`;
- zowel bewegingsconfidence als betrouwbaarheid is minimaal `Matig`;
- target-, storm- en notificatieservice-identiteit zijn geldig en begrensd.

Unknown, stale, blocked, onvoldoende gedekte of niet lokaal toegelaten data kan
geen melding veroorzaken. De drie fasen zijn:

| Fase | ETA |
| --- | --- |
| `vroeg` | meer dan 45 tot en met 90 minuten |
| `nabij` | meer dan 15 tot en met 45 minuten |
| `imminent` | 0 tot en met 15 minuten |

## Dedupe, escalatie en herstel

Na een geslaagde notify-call bewaart Home Assistant een begrensde persistente
claim voor target + storm + fase. Daardoor veroorzaakt een herstart geen
dubbele melding. Een hogere fase mag onmiddellijk escaleren; dezelfde of een
lagere fase wordt niet opnieuw verstuurd. Voor een ander systeem geldt per
target een cooldown van dertig minuten.

Een actuele, niet-bedreigende toestand moet tien minuten bevestigd blijven
voordat het target opnieuw wordt gewapend. Unknown, stale of geblokkeerde data
geldt nooit als clear en kan dus geen kunstmatige re-arm veroorzaken. Na een
bevestigde clear kan dezelfde storm/fase opnieuw melden zodra ook de cooldown
verstreken is. Persistente records ouder dan zeven dagen en records boven de
harde target- en claimgrenzen worden verwijderd.

## Notify-call, event en diagnose

De dispatcher controleert eerst of de exact geconfigureerde
`notify.<service>` bestaat. Alleen na een foutloze, blocking Home
Assistant-servicecall wordt een sent-claim gemaakt en het exacte event
`storm_tracker_v3_warning_sent` gepubliceerd. De bestaande
kalibratielistener bewaart precies die provider-neutrale snapshot.

Ontbrekende service, ongeldige configuratie of een fout in de notify-call geeft
geen sent-claim en geen sent-event. De volgende cyclus mag opnieuw proberen.
De actuele, privacyveilige diagnose staat in:

```text
hass.data["storm_tracker_v3"]["warning_dispatch_diagnostics"]
```

De diagnose publiceert geen servicenaam, target-ID, storm-ID, adres of geheim.
Het interne sent-event bevat alleen begrensde nowcastvelden; coördinaten worden
tot drie decimalen afgerond. De notify-payload bevat geen coördinaten,
providerpayloads, credentials of adressen.

## Uitschakelen en rollback

Zet `warning_notifications_enabled` terug op `false` in de legacy-opties en
laat de config-entry herladen. Er wordt dan geen notify-service aangeroepen en
geen sent-event gepubliceerd. De meteorologische verwerking en sensoren
blijven onafhankelijk werken. Omschakelen naar worker-only verwijdert de
legacy-only notificatievelden via de bestaande gesloten reconfigure-projectie.

Volg voor iedere installatie of rollback daarnaast ongewijzigd
`docs/HA_DEPLOY_EN_HERSTELPROCEDURE.md`.
