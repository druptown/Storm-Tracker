# KNMI native real-time QPE capture

Status: worker-only kandidaat, lokaal getest, niet live gekoppeld en niet
gedeployed.

## Waarom dit product

De gekozen bron is het officiële KNMI-dataset
[`nl_rdr_data_rtcor_5m/1.0`](https://dataplatform.knmi.nl/dataset/nl-rdr-data-rtcor-5m-1-0).
KNMI beschrijft dit als HDF5-bestanden met vijfminutenaccumulaties die uit
radar worden afgeleid en met regenmeters worden gecorrigeerd. Het composiet
gebruikt Nederlandse, Belgische en Duitse radars en bestrijkt ook België. De
licentie is CC BY 4.0 en de catalogus vermeldt geen gebruiksbeperkingen.

Dit vervangt in vNext de oude interpretatie van gekleurde WMS-PNG's. Een
rendered image en een zelfgemaakte kleurklasse zijn geen kwantitatieve
neerslagmeting en mogen niet als meteorologische waarheid worden behandeld.

## Officieel transportcontract

De implementatie volgt de
[`Open Data API`](https://developer.dataplatform.knmi.nl/open-data-api):

1. lijst precies het nieuwste bestand met `maxKeys=1`, `orderBy=created` en
   `sorting=desc`;
2. vraag voor die bestandsnaam een tijdelijke download-URL op;
3. download het bestand via die tijdelijke URL zonder de KNMI
   `Authorization`-header door te sturen.

De API-key komt uitsluitend uit `STV3_KNMI_API_KEY` in de workeromgeving.
De jobpayload moet exact leeg zijn; hierdoor kunnen ook geneste sleutels,
tokens, credentials of download-URL's niet via de queue binnenkomen. De twee
control-plane JSON-responses zijn geen
weerartefacten. Vooral de tijdelijke URL bevat credentials en wordt daarom
nooit in provenance, receipts, artifacts of resultaatmetadata opgeslagen.

Beide API-responses moeten exact beantwoorden aan de officiële OpenAPI-
schema's `ListFilesResponse`, `FileSummary` en `FileDownload`. Extra of
ontbrekende velden, verkeerde types, onbegrensde bestandsgrootte, een andere
`lastModified`, een afwijkende `Content-Type` of een afwijkende gedownloade
bodylengte falen gesloten. Redirects zijn zowel op het API-endpoint als op de
signed object-URL verboden. Alleen publieke HTTPS-objecthosts uit de vaste
KNMI/object-storeallowlist op poort 443 worden aanvaard; userinfo, IP-literals,
fragmenten en niet-toegestane hosts worden geweigerd.

KNMI waarschuwt dat buitensporig pollen misbruik is. De huidige kandidaat is
nog niet gescheduled. Bij latere activering geldt maximaal de productcadans
van vijf minuten; voor de snelste en zuinigste productie-ingang heeft KNMI's
Notification Service de voorkeur boven sneller pollen.

## Exact-byte volgorde

`storm_tracker_worker/knmi_native.py` voert de volgende harde volgorde uit:

1. begrensde, identity-encoded controleresponses;
2. veilige HDF5-bestandsnaam;
3. begrensde, identity-encoded HDF5-download;
4. **exacte HDF5-bytes duurzaam opslaan** in `RawArtifactStore` schema 3;
5. pas daarna de HDF5-container structureel openen;
6. alleen bij een geldige KNMI radaraccumulatie-identiteit, vaste producttijd,
   de werkelijk aangetroffen HDF-tag 3.6 en NL25-grid een complete
   capture met required role `hdf5` vastleggen.

Een ongeldige, verkeerde of beschadigde HDF5-response blijft dus hoogstens een
exact receipt. Ze wordt nooit een complete capture, nooit een neerslagveld en
nooit stilzwijgend `droog`.

De stabiele upstream-identiteit is dataset + versie + bestandsnaam. Een
providerbestand dat onder dezelfde naam andere bytes krijgt, veroorzaakt een
hard conflict. Een retry van hetzelfde job-ID zoekt eerst het duurzame receipt,
leest receipt plus exacte bytes in één gecombineerde authority-pass en voltooit
de inspectie/capture zonder netwerktoegang. Een volgend tijdslot krijgt een nieuw receipt dat
hetzelfde fysieke content-addressed object mag hergebruiken.

De volledige job, inclusief threadlock, procesoverschrijdende provider-capacity-
en slotlock,
authority-read, inspectie, raw-store en capturecommit, deelt een absolute
monotone deadline van 75 seconden. Een complete retry gebruikt
`load_capture_bundle_bytes` en leest/hash't zijn ene HDF5-member exact eenmaal.
Een atomair gepubliceerde, canonieke en begrensde validatieverdict bindt de
content-SHA aan de inspectorversie. Daardoor vereist een store-reopen geen
nieuwe HDF5-parse. Alleen structurele `ValueError`-afkeur wordt duurzaam
onthouden; timeout, transport- en ambigue HDF5-I/O-fouten worden nooit als
deterministische bronafkeur gecachet. De verdictmap heeft een vaste capaciteit,
veilige paden, file-lockserialisatie, file-fsync en op POSIX directory-fsync.

## Wat de structurele inspectie wel en niet bewijst

De inspectie leest uitsluitend de minimale HDF5-structuur en metadata:

- `/overview`: productgroep, HDF-tagversie, begin- en eindtijd;
- `/geographic`: rasterrijen en -kolommen;
- `/image1/image_data`: intern hard datasetobject, exacte `765 × 700`-vorm,
  begrensde unsigned dtype/chunks/filters en bytegrootte, zonder pixelwaarden
  te lezen.

Alle HDF5-links moeten intern en hard zijn. Externe/soft links, externe
datasetopslag, virtuele datasets, buitensporige objectaantallen en te diepe
groepsstructuren worden geweigerd. Productgroep, imageproduct,
`PRECIP_[MM]`, vijfminuteninterval en producttijd in de bestandsnaam moeten
onderling overeenstemmen. Dit is nog uitsluitend productidentificatie, geen
gecertificeerde kalibratiedecode.

De vernieuwde productspecificatie bevestigt bovendien de verplichte
`image2`-quality-index en `image3`-adjustment-factor. De capture bindt hun
exacte quantity, vorm en dtype structureel, maar decodeert nog geen waarden of
calibratie. Veilige contiguous of begrensd gechunkte storage en ondersteunde
compressiefilters blijven equivalent: fysieke HDF-opslaglayout is geen
meteorologische identiteit. Zie de KNMI-auteurspublicatie
[Open radar gauge-adjusted precipitation products](https://essd.copernicus.org/articles/17/4715/2025/).

Het tijdformaat volgt de KNMI HDF-tagconventie. De generieke
[`KNMI HDF5-tagdocumentatie`](https://cdn.knmi.nl/system/data_center_publications/files/000/072/301/original/hdftag38.pdf?1739444428=)
beschrijft versie 3.8, maar het actuele RTCOR-product declareert in de echte
payload versie 3.6. Het capturecontract volgt daarom de echte productbytes en
mag die versies niet verwisselen.
Producttijd en ontvangsttijd blijven afzonderlijke feiten.

Een read-only live golden op 2 augustus 2026 heeft het echte bestand
`RAD_NL25_RAC_RT_202608012345.h5` gecontroleerd: 39.925 bytes, SHA-256
`a138f091fb61181d2f83e5892e0ba5db19088211d3fc26ccff41bb90b9927d8d`.
De payload declareert intern `RAD_NL25_COR_NA`, `PRECIP_[MM]`, HDF-tag 3.6,
start 23:40 UTC, einde 23:45 UTC, `765 × 700`, uint16 en gzip. De actuele
inspector en de bestandsnaam-/producttijdbinding accepteren exact deze bytes.
De tijdelijke download-URL en zijn querycredentials zijn niet vastgelegd.

Dit bewijst nog **niet**:

- de kalibratieformule, missing/out-of-image codes of fysieke pixelwaarden;
- de projectie en exacte gridgeometrie;
- per-pixel kwaliteitsvelden en onzekerheid;
- een nat/droog-besluit;
- cellen, systemen, beweging, snelheid of voorspelling;
- operationele toelating.

Daarom retourneert de kandidaat `decode_status=not_decoded`, geen
`observations`, geen overlay en `operational_admission_eligible=false`.

## Testbewijs

`tests/test_worker_knmi_native_capture.py` dekt onder meer:

- exacte bytes zijn aanwezig vóór de eerste HDF5-inspectie;
- API-authenticatie wordt niet naar de signed URL doorgestuurd;
- redirects, localhost/private-IP/host-substitutie en signed-URL SSRF falen
  gesloten;
- volledige OpenAPI-objecten, listing/downloadmetadata, responsegrootte en
  contenttype zijn onderling gebonden;
- key en signed query ontbreken uit resultaat en opgeslagen receipt;
- vreemde productgroep, 1D/transposed raster, nulduur, verkeerde HDF-tag,
  dtype, hoeveelheid, linksoort of producttijd houdt alleen een receipt over;
- opslagfout stopt vóór HDF5-inspectie;
- offline retry-hervatting na een crash direct na raw opslag en conflict bij
  gewijzigde bytes onder dezelfde naam;
- duplicate JSON keys, oversized bodies, onveilige bestandsnamen en geheime
  jobvelden falen gesloten;
- het capture-resultaat bevat geen regen- of droogclaim.

Aanvullende regressies bewijzen een store-reopen zonder HDF-reparse, exact een
geverifieerde memberread per complete retry, duurzame structurele afkeur,
timeout zonder afkeurcache en case-sensitive RTCOR-bestandsidentiteit.

Gerichte lokale stand na hardening: `60 passed`.

## Nog gesloten gates

De bestaande `knmi_fetch` blijft voorlopig naar het legacy WMS-pad wijzen. De
native kandidaat mag pas die servicereferentie vervangen nadat:

1. de onafhankelijke red-teamcontrole geen P0/P1-blokkers meer vindt;
2. de productspecifieke decoder gain/calibratie, masks, tijd, grid en CRS exact
   en bounded decodeert;
3. het afgeleide raster volledig aan raw receipt + capture + decoderversie is
   gebonden;
4. `CanonicalEvidenceStore` de decoderoutput als structureel bewijs bewaart
   zonder die automatisch meteorologisch toe te laten;
5. de worker één compacte operationele snapshot bouwt;
6. Home Assistant alleen die snapshot projecteert en geen providerdownload,
   HDF5-decode, fusie of tracking uitvoert;
7. replay-, corruptie-, disk-full-, latency-, thermische en soaktests groen
   zijn.

Daarnaast mag `created desc` nooit rechtstreeks als operationele tijd-head
worden gebruikt: een laat toegevoegd backfillbestand blijft bewaarbare raw
evidence, maar mag zonder producttijd-high-watermark/notification-contract de
actuele radarhead niet terugzetten.

Tot die gates klaar zijn, blijft de machineleesbare providerregistry voor
`knmi_fetch` correct op legacy/unwired staan.
