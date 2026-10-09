# Provider-qualityruntime

Status: lokaal en shadow-only. De module is byte-identiek aanwezig in de Home
Assistant- en workerkern, maar wordt door geen enkel live codepad geïmporteerd.

Implementatie:

- `custom_components/storm_tracker_v3/engine/provider_quality_runtime.py`;
- `storm_tracker_worker/tracking_core/engine/provider_quality_runtime.py`;
- `tests/test_provider_quality_runtime.py`.

## Doel

Een technisch geslaagde HTTP-request bewijst niet dat een provider het beste
actuele meteorologische beeld levert. De kwaliteitsruntime beoordeelt daarom
elk immutable frame afzonderlijk en bewaart alle beoordelingen, ook wanneer
een frame stale, incompleet, foutief of onvoldoende onderbouwd is.

De runtime maakt expliciet onderscheid tussen:

- bruikbaar als opgeslagen diagnostisch/replaybewijs;
- volledig genoeg om een kwaliteitsscore te berekenen;
- eligible als huidige bron;
- daadwerkelijk geselecteerd als beste huidige bron.

Een stale frame wordt niet verwijderd, maar kan nooit als huidige beste bron
worden geselecteerd.

## Expliciet beleid zonder providerprioriteit

`ProviderQualityPolicy` heeft geen defaults. De caller moet vooraf leveren:

- een policy-ID;
- exact één gewicht voor elke canonieke scorecomponent;
- per provider/product een freshnesscurve;
- maximale toegelaten toekomstige klokskew;
- tolerantie waarmee twee kwaliteitsscores als gelijk gelden.

De componenten zijn:

1. freshness;
2. completeness;
3. upstream quality;
4. geolocation consistency;
5. echo continuity;
6. operational health;
7. empirische replayperformance;
8. handoff readiness.

Elk componentgewicht is zichtbaar. Een component met positief gewicht maar
ontbrekende evidence maakt de totaalscore `None` en het frame niet-selecteerbaar.
Een ontbrekend component negeren kan alleen door vooraf zichtbaar gewicht nul
te geven. Zo ontstaat geen verborgen renormalisatie.

De empirische replaycomponent is een vooraf gedefinieerde index in `[0, 1]`,
niet zomaar een willekeurige ruwe skillscore. Wanneer hij aanwezig is, bewaart
de assessment verplicht metric-ID, eventaantal, corpus-SHA-256,
split-SHA-256 en evidence-SHA-256. Een losse waarde zonder die volledige
provenance wordt geweigerd.

## Freshness

Elke provider heeft twee expliciete leeftijden:

- `full_score_through_seconds`: freshnessscore blijft één;
- `current_until_seconds`: freshnessscore wordt lineair nul.

Daartussen daalt de score lineair. Na `current_until_seconds` is het frame
stale voor huidige selectie, ongeacht HTTP-, parser- of andere kwaliteit.
Een frame verder in de toekomst dan de expliciete skewtolerantie is eveneens
niet-selecteerbaar en blijft wel in de assessment staan.

## Score en selectie

Voor complete evidence is:

```text
score = som(componentwaarde * componentgewicht)
        / som(componentgewicht)
```

Parser- of fetchfouten maken het frame niet-selecteerbaar, maar de gemeten
componenten en score blijven auditbaar. De selector vergelijkt alleen volledige,
actuele en operationeel foutvrije frames.

De hoogste score wint. Bij een scoretie wint uitsluitend een aantoonbaar
nieuwer `observed_at`. Zijn score én geldigheidstijd gelijk, dan wordt de
uitkomst `TIED` en blijft de geselecteerde provider leeg; er bestaat geen
verborgen alfabetische providerprioriteit.

Inputvolgorde kan de uitkomst niet veranderen.

## Grenzen

Deze module:

- kiest geen wetenschappelijke gewichten;
- verzint geen ontbrekende QC-waarde;
- bewijst geen provider-skill;
- voert geen providerhandoff uit;
- verwijdert geen frame of weerobject;
- is niet live gekoppeld en niet gedeployed.

Voor live gebruik moeten policygewichten en freshnesscurves vooraf per product
worden verantwoord, complete componenten uit echte providerrecords worden
gevuld, replayperformance uitsluitend uit held-out onafhankelijke labels komen
en selectie plus handoff gedurende een shadowsoak worden gevalideerd.
