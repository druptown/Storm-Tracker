# Shadow-retentieplanner voor providerartifacts

`artifact_retention_planner.py` is een zuivere, deterministische
beslislaag. De Home Assistant- en workermodules zijn byte-identieke spiegels.
Geen van beide is in de live runtime geïmporteerd. De planner opent geen pad,
schrijft geen bytes, maakt geen archief en verwijdert nooit een artifact.

## Beslissingen

- `STORE`: de bron heeft een toegelaten contractklasse, de caller heeft een
  per-providerquota en de contractuele bronvermelding exact geconfigureerd, en
  globale quota, providerquota en vrije-ruimtereserve blijven intact.
- `HOLD`: juridische of contractuele feiten zijn onbekend, de
  `RetentionClass` is `CONTRACT_REVIEW_HOLD`, attribution of quota ontbreekt,
  of een onder legal hold geplaatste nieuwe revision botst met capaciteit.
  Contract- en licentie-onzekerheid draagt altijd expliciet
  `REVIEW_REQUIRED`.
- `ARCHIVE_REQUIRED`: opslaan kan pas nadat de gerapporteerde kandidaten door
  een andere, geautoriseerde laag zijn behandeld. Dit is geen automatische
  verwijderopdracht.
- `REJECT_CAPACITY`: de capaciteit is onvoldoende en er bestaan niet genoeg
  veilig archiveerbare revisions.

KMI en RainViewer blijven volgens de huidige providercontracten dus op
`HOLD/REVIEW_REQUIRED`. NOAA Enterprise Rain Rate, NOAA GOES en METAR vallen
in `PUBLIC_REPLAY_EVIDENCE`; DWD en KNMI in
`ATTRIBUTION_REPLAY_EVIDENCE`. Ook die toegelaten klassen krijgen nooit
impliciete toestemming: quota, reserve en attribution komen van de caller.

## Revision- en capaciteitsmodel

Iedere combinatie van provider, receipt-ID en revision-ID is een afzonderlijk
quota-item. Twee revisions met dezelfde receipt of dezelfde contenthash worden
niet samengevoegd in de boekhouding. Het plan bevat:

- artifactgrootte, ontvangst- en nominale tijd;
- contentchecksum, bronvermelding en secretvrije provenance;
- replaydependency-refcount;
- archiefstatus, archiefchecksum, verificatiestatus en archiefreceipt;
- legal hold en lokale aanwezigheid;
- actuele en geprojecteerde globale/providerbytes;
- vrije ruimte, reserve, geprojecteerde dagelijkse groei en days-to-reserve vóór
  en na de kandidaat.

Alle bytebudgetten gebruiken een positief signed-64-bit bereik. Nul,
negatieve waarden, niet-eindige tijden en overloop falen vóór een beslissing.

## Wanneer iets archiefkandidaat kan zijn

Een lokale revision is alleen selecteerbaar wanneer tegelijk:

1. het providercontract duurzaam bewaren toelaat;
2. geen legal hold actief is;
3. geen actieve replaydependency bestaat;
4. de archiefstatus `ARCHIVE_VERIFIED` is;
5. de checksumverificatie waar is;
6. de archiefchecksum exact aan de contentchecksum gelijk is;
7. een niet-lege, secretvrije archiefreceipt bestaat.

Elke niet-selecteerbare revision blijft met concrete blokkeerredenen in het
plan staan. De selectie is stabiel geordend op ontvangsttijd en daarna op de
volledige revisionidentiteit. Eerst wordt een eventuele providerquota opgelost,
vervolgens de globale quota of vrije-ruimtereserve.

## Harde grens van deze fase

Dit is nog **geen bewezen opslagprojectie** zonder een
**echte capture-size-inventaris** per provider, product, cadence en
bronrevision. De dagelijkse groei is een caller-aangeleverde meetwaarde; de
planner bewijst niet dat de huidige transports alle originele bytes al
vastleggen. Live capture, storage, archivering, quota-enforcement, deletie,
deploy en providertransport blijven bewust buiten deze shadowfase.
