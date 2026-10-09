# Virtual-probe workeradmissie

Status: lokale correctie en regressietest; niet live gedeployed en niet live
herstart.

## Operationele fout die hiermee wordt verklaard

Bij de read-only controle op 2026-07-30 rond 14:14-14:16 CEST was de worker
online en de wachtrij leeg. Het proces `virtual_probe_operational` meldde wel:

- `state=error`;
- `error=ValueError`;
- 40 mislukte jobs en 0 geslaagde jobs in de eerste statusopname;
- negen actieve virtuele verkenners in
  `sensor.stv3_virtuele_regenverkenners`.

De workerlog gaf voor opeenvolgende jobs exact deze stack:

```text
storm_tracker_worker/service.py:289 in _run_jobs
storm_tracker_worker/virtual_probe.py:66 in virtual_probe_operational
storm_tracker_worker/virtual_probe.py:29 in run_virtual_probes
ValueError: te grote verkennerpayload
```

De live workercode accepteerde maximaal acht items in `active_probes`. De
Home Assistant-kant stuurde alle negen bestaande verkenners mee. Daardoor was
de predicate `len(active_probes) > 8` zeker waar en werd de volledige job
afgekeurd voordat een verkenner of grondobservatie kon worden geëvalueerd.
De generieke fouttekst liet niet zien welke admissieregel faalde.

Dit is geen bewijs van slechte KMI-radardata en de brede regenzone veroorzaakte
deze specifieke fout niet. Het is een intern herstel-/overdrachtscontract dat
strenger was dan de toestand die Home Assistant legitiem kon bewaren.

## Lokale correctie

De grens van acht blijft ongewijzigd als operationeel planningsdoel. De
`VirtualProbeManager` plant dus geen nieuwe verkenners wanneer al acht of meer
verkenners actief zijn.

Voor herstel en rolling upgrades gebruikt de worker nu een afzonderlijke,
expliciete bovengrens:

```text
MAX_RESTORED_ACTIVE_PROBES =
    MAX_REGIONS * MAX_ACTIVE_PROBES
    = 8 * 8
    = 64
```

Daarmee kan de worker een tijdelijk samengevoegde bestaande toestand verwerken
zonder bestaande verkenners te verwijderen. Alle ontvangen, geldige
verkenner-ID's blijven in de diagnostiek aanwezig. Zodra evaluaties verkenners
afronden of annuleren, daalt de actieve set vanzelf. Boven 64 blijft de worker
fail-closed met een fout die ontvangen aantal en herstelgrens benoemt; er wordt
niets stil afgekapt.

Het resultaat bevat bovendien `processing_counts` met de ontvangen aantallen,
het operationele doel en de herstelgrens. Daardoor is een toekomstige
admissiefout controleerbaar zonder de inhoud van observaties of verkenners uit
de payload weg te laten.

De bestaande limieten van maximaal acht regio's en maximaal 2.000
grondobservaties zijn niet gewijzigd. Het MQTT-contract blijft daarnaast
begrensd op 1 MB gecodeerd en 32 MB verliesloos uitgepakt.

## Regressiebewijs

`tests/test_worker_virtual_probe.py` bootst de operationele fouttoestand na met
negen unieke, bestaande verkenners:

- vóór de correctie: `ValueError: te grote verkennerpayload`;
- na de correctie: succesvolle verwerking, `active_count=9` en exact dezelfde
  negen `probe_id`-waarden in de diagnostiek;
- 65 bestaande verkenners: expliciete fail-closed fout op herstelgrens 64;
- 2.001 grondobservaties: bestaande fail-closed observatiegrens blijft actief.

## Open live gate

De correctie is pas operationeel bewezen na een afzonderlijk toegestane
deploy/herstart en daarna een read-only controle waarin:

- de worker een nieuw runtime-build-ID meldt;
- `virtual_probe_operational.completed_jobs` stijgt;
- `failed_jobs` niet verder stijgt;
- `processing_counts.active_probes_received` zichtbaar overeenkomt met de
  aangeleverde toestand;
- verkenner-ID's en uitkomsten intact blijven.

Geen van die live acties is tijdens deze audit uitgevoerd.
