# HA desired-state controller — ongewirede worker-only kandidaat

## Status en afbakening

`custom_components/storm_tracker_v3/desired_state_controller.py` is een lichte,
volledig ongewirede control-plane kandidaat. De module registreert geen Home
Assistant-listener, entity, service of event en wordt nergens vanuit
`__init__.py`, `sensor.py` of `config_flow.py` aangeroepen. Deze wijziging is
daarom geen live cutover en verandert geen provider-, opslag-, reken- of
meteorologische code.

De kandidaat doet uitsluitend het volgende:

1. maximaal één actuele set van hoogstens 64 HA-targets en één kleine
   runtimesettings-set in geheugen houden;
2. authority bij de worker claimen;
3. de privacy-minimale desired-state-head opvragen;
4. met `desired_state_builder.build_desired_state` exact één canoniek
   compare-and-swap-document bouwen;
5. dat document via `worker_bridge.async_request_worker_job` toepassen.

Bij een onbeschikbare worker bestaat geen lokale provider- of rekenfallback.
De controller houdt alleen de nieuwste begrensde inputs, één `dirty`-vlag en,
indien een applyantwoord onzeker werd, hoogstens één canonieke poging van
maximaal 64 KiB bij. Er ontstaat geen job- of positiebacklog.

## Identiteit, authority en CAS

De aanmaker levert een stabiele `producer_id`. Per controllerboot wordt één
nieuwe `claim_id` afgeleid van die producer en een boot-token. Het boot-token
is standaard een eenmalige UUID; tests of een toekomstige integratiewiring
kunnen een expliciete `boot_id` injecteren. De ruwe boot-token verschijnt niet
in een workerpayload of diagnostic.

De eerste cyclus is strikt:

```text
desired_state_claim -> desired_state_snapshot -> desired_state_apply
```

De claim levert de fencing-`authority_epoch`. De snapshot bevat alleen
controlem metadata en nooit targets of coördinaten. De volgende toestand bouwt
altijd voort op de teruggegeven actieve SHA, of op `null` wanneer de worker nog
geen head heeft. Onder een nieuw geclaimde authority begint generation op `1`.
Latere applies binnen dezelfde producer en epoch gebruiken steeds de actieve
generation plus één.

Dit werkt ook wanneer een nieuwe HA-boot authority claimt boven een bestaande
head van een vorige boot: de oude SHA blijft de `previous_state_sha256`, terwijl
producer, epoch en generation bij de nieuwe authority horen.

## Deterministische job-ID's en herstel

Alle job-ID's zijn begrensd en bevatten geen targetdata:

- claim: hash van producer plus boot-claim;
- snapshot: dezelfde boot-hash plus een begrensd lokaal volgnummer;
- apply: prefix plus de eerste 32 hextekens van de canonieke state-SHA.

Een transportretry van dezelfde logische apply hergebruikt exact hetzelfde
document en dezelfde job-ID. Na timeout of reconnect claimt de controller met
dezelfde boot-claim en leest hij opnieuw de workerhead. Staat de onzekere
state-SHA al actief, dan wordt het verloren antwoord lokaal als ontvangen
geboekt. Staat de oude CAS-head nog actief, dan wordt dezelfde apply
idempotent herhaald.

Een herkenbaar CAS-, generation- of fencingconflict veroorzaakt maximaal één
nieuwe privacy-minimale snapshot en één rebuild vanaf de actuele head. Een
tweede conflict of ieder ander job-/protocolprobleem blokkeert automatische
retry. De nieuwste state blijft `dirty` totdat een expliciete worker-reconnect
of flush een nieuwe begrensde cyclus start; er is dus geen snelle foutlus.

## Coalescing, sequencing en posities

De caller geeft telkens een volledige targets/settings-snapshot. De bestaande
builder valideert en canonicaliseert die eerst; een 65e target, extra velden,
ongeldige getallen of credentialvormige inhoud falen vóór een workerrequest.
Een semantische SHA over alleen de genormaliseerde targets en settings maakt
volgorde-onafhankelijke duplicaten zichtbaar.

- gelijke updates worden zonder workerjob gecoa­lesced;
- wijzigingen binnen het debouncevenster vervangen alleen de ene actuele
  snapshot;
- er kan maximaal één claim/snapshot/apply-request tegelijk lopen;
- een wijziging tijdens een in-flight apply wordt na die apply precies één
  volgende state, met de zojuist geactiveerde SHA als CAS-basis;
- een oudere `position_observed_at` overschrijft nooit een nieuwere positie;
- een unavailable update zonder nieuwe positietijd behoudt de laatste bekende
  coördinaten en tijd, maar mag `available=false` publiceren;
- een gelijke tijd met afwijkende coördinaten wordt eveneens als regressieve,
  conflicterende positie gefilterd.

Targetverwijdering blijft mogelijk omdat iedere invoer een volledige snapshot
is. Er wordt geen onbeperkte historiek van verwijderde targets bijgehouden.

## Lifecycle

De kandidaat heeft bewust alleen een expliciete, nog ongeregistreerde API:

- `async_start(targets=..., settings=...)`: eerste volledige publicatiepoging;
- `async_update(targets=..., settings=...)`: nieuwste snapshot accepteren en
  debouncen;
- `async_flush()`: debounce overslaan en tot de volgende veilige grens wachten;
- `async_worker_reconnected()`: een dirty state opnieuw claimen/controleren;
- `async_stop()` / `async_unload()`: timer en in-flight request annuleren.

Een toekomstige wiring moet zelf HA-state-events vertalen naar het gesloten
targetcontract en zelf workerstatus-events naar `async_worker_reconnected`
routeren. Die wiring hoort pas na afzonderlijke review in de integratie.

## Privacy en diagnostics

De module logt niets. `diagnostics()` heeft een vaste vorm en bevat uitsluitend:

- toestand-, lifecycle-, dirty- en in-flight-vlaggen;
- aantallen en harde limieten;
- authority-epoch, generation en one-way state-SHA's;
- een begrensd fouttype en een vaste foutcode;
- verzadigende tellers.

Producer-ID, claim-ID, boot-token, target-ID, displaynaam, positie,
coördinaten, settingspayload en ruwe workerfouttekst worden niet opgenomen.
Ook een workerexception die coördinaten in zijn tekst bevat kan daardoor niet
naar diagnostics lekken.

## Harde grenzen

| Grens | Waarde |
|---|---:|
| Targets in de actuele snapshot | 64 |
| Canoniek desired-state-document | 64 KiB (buildercontract) |
| Worker metadatarespons | 8 KiB |
| In-flight controllerrequests | 1 |
| Onzekere applypogingen in geheugen | 1 |
| CAS-refreshes per cyclus | 1 |
| Debounce | 0–30 s, standaard 0,35 s |
| Requesttimeout | 0,05–120 s, standaard 35 s |
| Diagnostische tellers | verzadigen op 1.000.000.000 |

## Testbewijs

`tests/test_desired_state_controller.py` gebruikt uitsluitend een fake async
workerrequester en bewijst:

- claim/snapshot/apply-volgorde en een nieuwe authority boven een bestaande
  head;
- correcte previous SHA, epoch en generation;
- duplicaatcoalescing en een update tijdens in-flight apply;
- offline gedrag en expliciet reconnectherstel;
- identieke job-ID plus identiek document na een onzeker applyantwoord;
- CAS-conflict, nieuwe snapshot en rebuild;
- filtering van oude/out-of-order posities en unavailable last-known gedrag;
- cancellation/unload zonder achterblijvende controllertaak;
- vaste diagnostics zonder payload, coördinaten of fouttekst;
- afwijzing van 65 targets vóór enig workerrequest;
- AST-importisolatie: alleen standaardbibliotheek, `homeassistant.core`,
  `desired_state_builder` en `worker_bridge`, zonder provider-, workercompute-,
  opslag-, filesystem-, HTTP- of MQTT-import in deze module.

