# Open-Meteo exact-raw shadow capture

`storm_tracker_worker.openmeteo_native` is een inert worker-side
capturecontract voor `model.openmeteo.region`. Het is nog niet geactiveerd of
in de service gewired.

De input is uitsluitend de pseudonieme queryprojectie van
`AcquisitionQueryAuthority`: querysleutels, soort target en coördinaten. Een
target-ID of displaynaam gaat niet mee naar het childproces. Het verzoek is
deterministisch en gebruikt de publieke forecast-API zonder API-key.

## Bewaarde waarheid

- de exacte ontvangen JSON-bytes worden vóór iedere structurele inspectie
  content-addressed opgeslagen;
- requestparameters en pseudonieme queryvolgorde zijn via een SHA gebonden,
  maar staan niet in diagnostiek;
- alleen de exacte authorityvormen voor `acq:`-jobalias en `aqt:`-querysleutel
  worden aanvaard;
- redirects, iedere final-URL-drift, compressie, verkeerde content-type,
  dubbele of overmaatse headers, overmaatse of pathologische bytestreams,
  locatieaantal-, UTC-cadence-, responsevorm- en unitafwijkingen falen dicht;
- restart na durable responseopslag hervat zonder refetch en hervalideert
  object, receipt en capture; fysieke raw-tamper wordt geweigerd;
- resultaat en diagnostics bevatten geen coördinaten, querysleutels of
  meteorologische conclusie.

De request kiest `cell_selection=nearest` en `elevation=nan`. Daarmee wordt
geen landcel of statistische hoogtedownscaling toegepast; het ontvangen
gridmodel blijft beter reproduceerbaar. Open-Meteo's automatische best-match
kan nog steeds verschillende modellen en runs combineren. Daarom blijft dit
modelguidance en geen onafhankelijke radarmeting of ground truth.
Requestvorm en tijdsemantiek volgen de officiële
[Forecast API-documentatie](https://open-meteo.com/en/docs) en het
[OpenAPI forecast-schema](https://github.com/open-meteo/open-meteo/blob/main/openapi/forecast.yml).

## Attributie- en gebruiksauthority

De gebruikte publieke endpoint is vast gebonden aan een canonieke policy met
SHA-256 en compacte ID
`oma1:fcdeef70ad4c1273889c00577db71b669dd711102dbaa51efb831ca0b1a55212`.
De volledige policy staat in de immutable raw-receiptprovenance; request- en
capture-identiteit binden dezelfde policy. Het gewone resultaat toont alleen
de policy-ID en lekt geen query of coördinaat.

De op 2026-08-09 gecontroleerde policy legt vast: providercredit
`Open-Meteo`, data onder `CC BY 4.0` met bron-, licentie- en wijzigingsmelding,
en gebruik van de gratis endpoint uitsluitend voor niet-commerciële
persoonlijke Home Assistant-automatisering zonder API-key. De gepubliceerde
free-tiergrenzen zijn 600 calls per minuut, 5.000 per uur, 10.000 per dag en
300.000 per maand; provider-meting blijft bepalend. Dit is herleidbare
provenance, geen juridisch advies, rate-limiter of retentierecht.

De childpayload en lokale diagnostics bevatten geen target-ID of displaynaam,
maar de HTTPS-request moet de geautoriseerde coördinaten aan Open-Meteo sturen.
De provider vermeldt dat troubleshootinglogs requestcoördinaten kunnen
bevatten, niet met derden worden gedeeld en na 90 dagen worden verwijderd.
Deze externe privacy-impact is eveneens in de policy gebonden; vóór activatie
is een expliciete gebruikersdisclosure nodig.

Primaire bronnen: [Terms](https://open-meteo.com/en/terms),
[Licence](https://open-meteo.com/en/license),
[Pricing en attribution FAQ](https://open-meteo.com/en/pricing) en de
[CC BY 4.0-licentie](https://creativecommons.org/licenses/by/4.0/).

De afzonderlijke, nog ongewirede
[usage-, rate- en terms-authority](WORKER_OPENMETEO_USAGE_AUTHORITY.md)
reserveert providerbreed vier UTC-budgetten en geeft pas daarna een duurzame
eenmalige fetchpermit. Deze native capture roept die sidecar nog niet aan.

## Atomair activatiebesluit van 2026-08-09

De 11-naar-12 activatie is fail-closed gestopt. De huidige workload heeft
globale targetauthority, vraagt een gemengd profiel van 19 variabelen en
gebruikt JSON met automatische `best_match`. De vaste selector `icon_eu`
bestaat officieel, maar is alleen voor Europa gedocumenteerd,
`lightning_potential` is ICON-D2-only en de JSON-response bevat geen
model-ID. Daardoor kan de vereiste vaste upstream modelbron niet uit de
huidige exacte raw bytes worden bewezen. Ook blijft de upstream modelcredit
in de attributionpolicy expliciet onopgelost.

Het volledige, machineleesbare gatebewijs staat in
[WORKER_OPENMETEO_ACTIVATION_BLOCKER.json](WORKER_OPENMETEO_ACTIVATION_BLOCKER.json).
Activation, child, recovery, capacityprofielen, terminal router en
raw-manifest-auditcatalogus blijven daarom exact op 11; er is geen partiele
Open-Meteo-wiring toegevoegd. Herbeoordeling vereist eerst een numeriek
begrensde modeldomain-authority, een modelspecifiek variabelenprofiel en
responsegebonden modelbewijs. Een eventuele FlatBuffers-variant is een nieuw
exact-raw contract en valt buiten deze bounded stap.

De atomische blocker-, native-, usage-, queryauthority-, plan-, runtime-,
child-, recovery-, capacity-, raw-audit- en terminal-routerregressie staat op
308/308 groen met warnings als errors. Daarbij is geen netwerkrequest gedaan.

## Niet geclaimd

- geen huidige regen/droogte;
- geen volgende dreiging;
- geen modelskill;
- geen provideradmission;
- geen operationele snapshot;
- geen recht op onbegrensde retentie.

## Lokaal bewijs van 2026-08-09

De gerichte module- en queryauthoritymatrix staat op 67 groene tests met
warnings als errors. De bredere raw-store-, acquisition-, recovery-, router-
en servicematrix staat op 298 groen en 1 platformskip omdat symlinkaanmaak
niet beschikbaar was. Een eerder begrensde
publieke live-smoke voor één Belgisch
modelpunt leverde 2.187 exacte bytes met SHA-256
`cf536850da55433a810b39406b6e732503f7869ab072cc3b31d887fa41309e8c`.
De response had exact 13 kwartierstappen, 6 uurstappen en 19 gevraagde
variabelen; haar structurele SHA is
`9208f9a7ab30a29017c46ed271253a5c3b86204f68c67dcee587e5e72a8a7ad5`.
De huidige aangescherpte inspector hervalideerde die bewaarde objectbytes en
dezelfde tijdalignment read-only, zonder een nieuwe netwerkcall. In de
oorspronkelijke smoke-run hervalideerde een verse store-instantie het toen
vastgelegde receipt zonder refetch. Nieuwe offline tests bewijzen afzonderlijk
de huidige policy-gebonden restart. Dit bewijst transport en contractreplay,
niet de modelskill.

Vóór activatie zijn nog nodig: gesloten atomische
activation/child/recoverybinding met de inerte usage-authority, hercontrole
van de dan geldende gebruiksvoorwaarden, zichtbare attribution- en
modelbronresolutie,
privacydisclosure, retentiebeleid, meerdere model-specifieke held-out replays,
resource-soak en een expliciete scheiding tussen best-match guidance en
radar/ground evidence.
