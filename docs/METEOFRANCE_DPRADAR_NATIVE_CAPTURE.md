# Météo-France DPRadar: exacte native capturekandidaat

Status: **volledig ongewired, capture-only, geen kwantitatieve decoder**

Auditdatum: **2 augustus 2026**

## Besluit

De bestaande workerroute `meteofrance_radar_fetch` vraagt het juiste officiële
500 m-product op, maar decodeert het HTTP-body meteen met de gedeelde
ODIM-parser en bewaart alleen afgeleide JSON. Daardoor is het originele
Météo-France-bestand niet replaybaar en is de parseraanname niet te auditen.

Er is nu een lokale schema-3-kandidaat in
`storm_tracker_worker/meteofrance_native.py`. Die bewaart exact:

1. het officiële catalogusantwoord voor
   `METROPOLE / LAME_D_EAU`;
2. het daaruit geselecteerde oorspronkelijke 500 m-HDF5-bestand
   `IPRN20_C_LFPW`.

Beide bodies worden vóór structurele inspectie opgeslagen. OAuth-responses,
Bearer-tokens, application credentials en Authorization-headers worden nooit
opgeslagen. De kandidaat leest geen rasterpixels en publiceert geen nat-,
droog-, cel-, forecast- of toelatingsfeit.

## Wat officiële primaire bronnen bewijzen

### Product en transport

De officiële Météo-France-handleiding voor de gerichte radar-API vermeldt:

- realtime radar, met een API-diepte van 20 uur en zonder archieffunctie;
- actualisatie van mozaïeken om de vijf minuten;
- slechts het meest recente product is via het productendpoint beschikbaar;
- het catalogusendpoint
  `/mosaiques/{zone}/observations/{observation}` publiceert de producttijd in
  `validity_time`;
- de 500 m-waterlaag voor `METROPOLE` is HDF5 (`application/x-hdf`);
- het exacte downloadendpoint is
  `/public/DPRadar/v1/mosaiques/METROPOLE/observations/LAME_D_EAU/produit?maille=500`.

Bron: [API Ciblée Radar (Météo-France Open Data)](https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/853639381/API%2BCibl%2Be%2BDonn%2Bes%2BRadar%2BEN).

### Meteorologische productspecificatie

De officiële technische productbeschrijving, versie 18 maart 2025, benoemt
het product als:

- referentie `IPRN20_C_LFPW`;
- nationale mozaïek voor Europees Frankrijk;
- vijfminuten-`lame d'eau` (neerslagaccumulatie);
- HDF5;
- 500 m;
- 3472 × 3472 pixels;
- neerslagwaarden in honderdsten millimeter;
- een kwaliteitscode van 0 (onbruikbaar) tot 100 (uitstekend);
- HDF5 bevat daarnaast informatie over netwerkstatus en radardefinities;
- ontbrekende waarde 65535 in de beschreven accumulatieprocedure.

De bestandsnaam in de HTTP-response heeft officieel de vorm
`T_{productreferentie}_YYYYMMDDhhmmss.h5`; het tijddeel is de UTC-acquisitietijd.

Bron: [Descriptif données Radar v1.2 (Météo-France, PDF)](https://donneespubliques.meteofrance.fr/client/document/descriptiftechnique_radar_donneespubliques_v1-2_20250318_404.pdf).

### Hergebruik en opslag

Météo-France plaatst de publieke radardata zonder vergoeding onder de Franse
Licence Ouverte Etalab 2.0. Reproductie, kopie, transformatie en verspreiding
zijn toegestaan met bron- en actualisatiedatumvermelding. Daarmee is duurzame
exacte opslag voor replay/evidence toegestaan, mits de toepassing
`Météo-France` en de update-/producttijd vermeldt.

Bronnen:

- [Données radars – conditions d'accès](https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/670924818/Donn%2Bes%2Bradars);
- [Droits de ré-utilisation et attribution](https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/457803314/Quels%2Bsont%2Bles%2Bdroits%2Bde%2Br%2B-utilisation%2Bet%2Bcomment%2Bsourcer%2Bles%2Bdonn%2Bes%2Bpubliques%2Bde%2BM%2Bt%2Bo-France).

## Exacte capture-authority

De complete capture heeft twee verplichte rollen:

| Rol | Exact artifact | Selectie/identiteit |
|---|---|---|
| `catalog_json` | ongewijzigde JSON-response van het officiële observatiecatalogusendpoint | exact één HTTPS-link op de officiële host met `maille=500`, HDF-mediatype, `validity_time` op vijfminutenrooster en officiële attributie |
| `hdf5` | ongewijzigde HTTP-body van het officiële productendpoint | `Content-Disposition` moet exact `T_IPRN20_C_LFPW_YYYYMMDDhhmmss.h5` geven en de bestandsdatum moet gelijk zijn aan catalogus-`validity_time` |

De capture bindt beide SHA-256-digests, beide immutable receipts, de officiële
productreferentie, bestandsnaam en UTC-producttijd in één atomisch
schema-3-capturemanifest. Een gedeeltelijke poging mag losse receipts
achterlaten maar wordt nooit als complete capture gepresenteerd. Een complete
retry is netwerk- en credentialvrij.

Transport is begrensd en fail-closed:

- uitsluitend vaste HTTPS-hosts en -paden;
- geen redirect;
- geen transportcompressie;
- verplichte en gecontroleerde `Content-Length`;
- maximaal 512 KiB catalogus en 40 MiB HDF5;
- één totale deadline van maximaal 75 s;
- begrensde JSON-diepte en -objecttelling;
- begrensde HDF5-objecttelling en datasetrank;
- de containerinspectie vereist minstens één 3472 × 3472-dataset, maar leest
  nooit een datasetwaarde.

## Waarom er bewust nog geen kwantitatieve decoder is

De officiële bronnen bewijzen de fysieke productklasse, nominale resolutie,
accumulatieduur, globale waarde-eenheid en kwaliteitsrange. Ze bewijzen in de
gevonden publieke documentatie niet voldoende exact:

1. welke HDF5-paden de accumulatie, kwaliteit, radarstatus en
   radardefinities bevatten;
2. welke quantity-identifiers en per-dataset gain/offset-attributen gelden;
3. het volledige native CRS en de exacte pixel-geotransform;
4. de precieze scheiding tussen `missing`, `undetect`, geldige nul en buiten
   dekking in elk HDF5-veld;
5. hoe de 0–100 pixel-QC en netwerkstatus samen operationele bruikbaarheid
   bepalen;
6. of het concrete HDF5-bestand ODIM is en, indien ja, welk ODIM-profiel exact
   geldt.

Daarom mag de bestaande generieke aanname
`dataset1/data1/data + gain/offset + factor 12` niet als gecertificeerde
Météo-France-decoder worden beschouwd. De nieuwe kandidaat meldt expliciet
`blocked_incomplete_hdf5_semantics`, `weather_state=unknown`,
`pixel_values_read=false` en `operational_admission_eligible=false`.

## Tests en evidence

`tests/test_worker_meteofrance_native_capture.py` controleert onder meer:

- de officiële gedocumenteerde catalogusvorm;
- exacte, secretvrije opslag van catalogus en HDF5;
- opslag vóór inspectie;
- productreferentie, bestandsnaam en tijdcrosscheck;
- strikte URL-, mediatype-, duplicate-key- en gridafwijzing;
- crashpunten vóór/na beide receipts en capturecommit;
- netwerkloze deterministische retry;
- afwijzing van credentials in jobpayloads;
- metadata-only HDF5-inspectie;
- afwezigheid uit live service, scheduler en capture-registry.

De fixture
`tests/fixtures/meteofrance_dpradar_official_catalog_example.json` is een klein
contractvoorbeeld afgeleid van de officiële API-documentatie. Het HDF5-bestand
in de tests is een synthetische containerfixture en **geen echt Météo-France-
weerframe**. Er is dus nog geen claim van live golden-validatie.

## Resterende harde gates

Voor een decoder of operationele aansluiting zijn minimaal nodig:

1. één geautoriseerde echte schema-3-capture bij droge, stratiforme en
   convectieve situaties;
2. inventarisatie van alle HDF5-objecten/attributen zonder pixels te
   herinterpreteren;
3. productspecifiek, officieel of onafhankelijk verifieerbaar
   veld-/CRS-/nodata-/QC-contract;
4. afzonderlijke begrensde decoder in hard child-process met CPU-, RSS- en
   deadlinegrenzen;
5. checksumgebonden derived receipt en kwantitatieve evidence bridge;
6. held-out vergelijking met pluviometers en onafhankelijke radarbronnen;
7. NUC-soak en bewezen nulimpact op Home Assistant/Zigbee2MQTT;
8. pas daarna shadowpublicatie, canary en expliciete cutoverbeslissing.

Geen van deze gates wordt door de capturekandidaat overgeslagen.
