# Recursieve CellTrack-bewegingsschatter

Status: lokale shadow-challenger, niet live gewired en niet gedeployed.

`recursive_track_estimator.py` bestaat byte-identiek in de Home
Assistant-kern en de worker. De schatter verwerkt uitsluitend detecties
waarvan een externe associatiestap al heeft beslist dat ze bij dezelfde
`CellTrack` horen. Hij beslist dus niet zelf over celidentiteit.

## Twee verschillende waarheden blijven zichtbaar

Iedere revisie bewaart afzonderlijk:

- de exacte lineaire verplaatsing tussen de laatste twee echte posities;
- de recursief geschatte state `[x, y, vx, vy]` voor voorspelling;
- de volledige 4×4 statecovariantie;
- de innovation `[dx, dy]` van de nieuwe meting;
- de normalized innovation squared als diagnose, nooit als afkeurpoort;
- alle ondersteunende detection-ID's, policy-ID en policyfingerprint.

Een extreme intervalvector wordt niet begrensd, herschreven of verwijderd.
Ook een zeer grote innovation wordt geassimileerd zodra de caller de detectie
met de track heeft geassocieerd. De hoge innovation blijft als controleerbaar
bewijs staan.

## Procesmodel

De caller moet zonder defaults leveren:

- de initiële velocity;
- de volledige initiële 4×4 covariance;
- een benoemde white-acceleration spectral density in `m²/s³`.

De voorspelling gebruikt een constant-velocity transition en de analytische
continuous-white-acceleration process covariance. Een meetupdate gebruikt de
door `CellDetection.position_sigma_m` geleverde isotrope
meetcovariantie. Covariance wordt met de Joseph-vorm bijgewerkt en iedere
matrix wordt op eindigheid, symmetrie en positieve semidefinietheid
gecontroleerd.

Een voorspelling door een detectiegat maakt geen fictieve observatie aan en
verandert de lijst met ondersteunende detection-ID's niet.

## Wat lokaal bewezen is

- extreme snelheid blijft exact als laatste interval beschikbaar;
- iedere reeds geassocieerde detectie wordt geassimileerd;
- een regelmatig zes-frame spoor convergeert naar zijn constante velocity;
- onzekerheid groeit bij langere voorspellingshorizon;
- grid-, tijd-, duplicaat- en policyconflicten falen gesloten;
- replay is deterministisch en state is immutable;
- HA- en workermodule zijn byte-identiek.

## Wat nog niet bewezen is

- onafhankelijke cell-identity- en bewegingslabels ontbreken;
- process noise is nog niet event-held-out gekalibreerd;
- vergelijking met exact last-two, optical flow en persistence op echte
  cases ontbreekt;
- de schatter is niet aangesloten op live ingest, duurzame snapshots,
  WeatherSystem-beweging, forecast of dashboard.

Daarom mag de recursieve state nog geen operationele waarschuwingen vervangen.
De exacte twee-positievector blijft de observeerbare baseline totdat held-out
bewijs aantoont welke schatter per horizon betrouwbaarder is.
