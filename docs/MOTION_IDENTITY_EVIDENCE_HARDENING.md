# Motion- en identiteitsbewijs in de offline replay

## Besluit

De extreme snelheden uit
`.analysis/kmi_region1_sparse_pyramidal_replay_20260730.json` zijn geen
meting van de snelheid van de KMI-regenzone en geen bewijs dat de actuele
KMI-data slecht is. Ze zijn de intervalafstanden van de **one-best
associatiehypothese** die een experimentele replay koos wanneer meestal geen
bruikbare object-motion beschikbaar was.

Deze hardening verandert geen detectie, rasterwaarde of kandidaat. Ze zorgt
ervoor dat een wiskundig verplichte solverkeuze niet langer als zekere
meteorologische celidentiteit kan worden gelezen.

## Wat de onderzochte replay werkelijk gebruikte

- 36 opeenvolgende KMI-cacheframes met een nominale cadence van 600 seconden;
- `pysteps_frame_cache`: een genormaliseerde Storm Tracker-cache, geen exacte
  upstream KMI-bytes;
- een begrensde analyseweergave van 112 × 177 pixels, afgeleid van een
  cachegrid van 636 × 1005 cellen;
- max-pooling per bronindex-bin, met bekende invloed op lokale extrema,
  oppervlak, centroid en connectiviteit;
- 35 opeenvolgende frameparen;
- 13.195 Shi-Tomasi featurekandidaten, waarvan 189 na ruimtelijke selectie;
- 46 bruikbare Lucas-Kanade-vectoren en 143 mislukte vectoren;
- 262 object-motionaggregaties:
  - 9 `available`;
  - 29 `unavailable_insufficient_usable_vectors`;
  - 224 `unavailable_no_selected_features`.

De 253 onbeschikbare aggregaties kregen in de test een expliciete
onzekerheidsplaceholder met `vx=0`, `vy=0`, een snelheidssigma van 100 m/s en
een residual-positiessigma van 100 km. Die nulvector is nadrukkelijk **geen
gemeten stilstand**. De brede covariantie maakt de motionterm juist weinig
informatief.

Na hardening blijft die placeholder uitsluitend bestaan om de
covariantie-/predictioncontracten volledig en auditbaar te houden. Wanneer
`MotionEvidenceRecord.status != available`, is zijn bijdrage aan de
assignmentcost exact nul. De gemeten Mahalanobis-feature blijft in de
kandidataudit staan, maar mag de ranking niet beïnvloeden alsof ze echte
motion-evidence was.

## Waarom toch zeer grote gekozen intervalafstanden ontstonden

1. De volledige vorige × huidige kandidatengraaf werd correct behouden.
2. Voor 253 van 262 objectvoorspellingen kon sparse optical flow geen
   objectvector leveren.
3. De brede onzekerheidsplaceholder maakte veel geografisch verschillende
   kandidaten weinig onderscheidbaar op motion.
4. Het caller-costmodel bleef oppervlak en niet-overlap rangschikken.
5. Vaste birth- en deathkosten van elk 6 maakten een match geregeld goedkoper
   dan een afzonderlijke death plus birth.
6. De globale solver moet altijd een exact one-best antwoord geven. Dat is een
   optimalisatie-uitkomst, geen identiteitsbewijs.
7. Boven de ingestelde top-k-capaciteit van acht huidige detecties leverde de
   schaalbare solver alleen one-best. In die overgangen bestond dus zelfs geen
   berekende tweede hypothese waarmee zekerheid kon worden beoordeeld.

Het gevolg in het oude rapport:

- 64 voorlopige tracks;
- 223 gekozen matches;
- mediane gekozen intervalspeed 446,47 km/h;
- p95 1.801,43 km/h;
- maximum 2.343,92 km/h.

Deze scalars blijven nuttig als diagnose van de associatiemethode. Ze mogen
niet worden gepresenteerd als de snelheid van het weer of als KMI-fout.

## Nieuwe pure shadow-contracten

`engine/motion_identity_evidence.py` voegt een verliesvrije,
fail-closed auditlaag toe:

- `MotionEvidenceRecord` draagt per vorige detectie expliciet
  `available`, `unavailable` of `not_assessed`;
- iedere complete edge blijft als `IdentityCandidateAudit` bewaard, inclusief
  gekozen costcomponenten, afstand, intervalspeed, Mahalanobis-term,
  oppervlakverhouding, footprint-IoU en flowprovenance;
- iedere door de begrensde solver behouden oplossing blijft als
  `AssignmentHypothesisAudit` beschikbaar;
- iedere match uit one-best krijgt een `MatchIdentityAssessment`;
- ontbrekende motion, ontbrekende alternatieven en betwiste alternatieven
  leiden expliciet tot `indeterminate_*`;
- zelfs een goed ondersteunde hypothese heet maximaal
  `supported_hypothesis_not_validated`;
- `confirmed_identity` blijft zonder onafhankelijke waarheid altijd `false`;
- onbeschikbare motion krijgt exact nul costgewicht, zonder het feature of de
  kandidaat te verwijderen;
- `speed_cap_kmh` blijft `null`;
- `candidate_rejection_by_speed` blijft `false`.

De replay behoudt zijn voorlopige tracktoewijzingen voor vergelijkbaarheid,
maar exporteert nu expliciet:

- `assignment_semantics =
  one_best_complete_graph_hypothesis;not_confirmed_meteorological_identity`;
- `chosen_speed_semantics =
  one_best_assignment_hypothesis_interval_speed;not_verified_cell_motion`;
- `confirmed_identity_match_count = 0`;
- aantallen onbepaalde en slechts ondersteunde-ongevalideerde matches;
- volledige kandidaat- en oplossingsaudits, of in compacte output de
  fingerprints, aantallen, oplossingen en matchbeoordelingen.

## Read-only herrun na hardening

Dezelfde 36-frame KMI-sequence en dezelfde caller-policy zijn opnieuw
uitgevoerd naar
`.analysis/kmi_region1_motion_identity_hardened_replay_20260730.json`.
De detecties, kosten en one-bestkeuzes zijn bewust niet veranderd:

- 64 voorlopige tracks en 223 one-best matches;
- mediaan 446,47 km/h, p95 1.801,43 km/h en maximum 2.343,92 km/h voor de
  **hypothese-intervallen**;
- 3.282 complete edgekandidaten bewaard over 30 niet-lege
  frameovergangen;
- 56 solveroplossingen bewaard;
- 17 beoordeelde overgangen zonder solveralternatief en 13 met minstens één
  behouden alternatief;
- 0 bevestigde identiteitsmatches;
- 0 ondersteunde-maar-ongevalideerde matches;
- alle 223 gekozen matches expliciet onbepaald:
  - 215 wegens onbeschikbare object-motion;
  - 7 wegens beschikbare motion maar geen solveralternatief;
  - 1 wegens betwisting binnen de behouden alternatieven/caller-costdelta.

Dat de oude speedscalars exact gelijk blijven is een belangrijke
verliesvrijheidscontrole: de hardening verbergt de extreme uitkomsten niet,
maar corrigeert hun wetenschappelijke betekenis.

De calibratiedatabase bleef byte-identiek:

`177d4704d665000722dbd131f3db03a238b26de072ce32bde6ea4650815cb263`
voor en na de read-only replay.

### Gevoeligheid: onbeschikbare motion volledig neutraal

Een tweede read-only run zette de motion-cost bij alle expliciet
onbeschikbare motionrecords exact op nul. De detecties, complete kandidaatset
en birth/deathpolicy bleven gelijk. Het reproduceerbare eindartefact is
`.analysis/kmi_region1_motion_identity_neutral_v2_replay_20260730.json`
(SHA-256
`dc22f78eb79d2c09e6b9651b4b4632d6eac9e1ecd1d0ee774f93bfc6046e6497`).

- track- en matchaantallen bleven 64 en 223;
- de hypothese-mediaan steeg van 446,47 naar 880,09 km/h;
- p95 steeg van 1.801,43 naar 2.736,64 km/h;
- het maximum steeg van 2.343,92 naar 2.978,60 km/h;
- alle 223 matches bleven onbepaald en 0 werden bevestigd.

Dit is geen meteorologische verslechtering en geen KMI-resultaat. Het toont
juist aan dat de oude brede nulplaceholder nog een arbitraire
afstandvoorkeur in de ranking aanbracht. Zodra die niet-bestaande motioninfo
neutraal wordt, sturen vooral vorm, onbewogen footprint-IoU en vaste
birth/deathkosten de gedwongen one-bestketen. Zonder onafhankelijke waarheid
kan geen van beide speedreeksen als correct worden gekozen.

### Tie-safe exacte one-bestsolver

De volledig neutrale motioncost maakte veel edgekosten gelijk en legde een
implementatiefout in de vroegere residual Bellman-Ford/min-cost-flowroute
bloot: een predecessorcyclus kon de solver laten stoppen. De schaalbare
one-bestroute gebruikt daarom nu een exacte deterministische Hongaarse
toewijzing (`stv3-exact-bounded-global-assignment-v2`):

- alle geldige match-, birth- en deathedges blijven in de augmented matrix;
- structureel ongeldige augmented edges krijgen uitsluitend intern een
  begrensde sentinel na numeriek veilige schaling;
- negatieve, grote en gelijke caller-costs blijven ondersteund;
- er is geen snelheids- of afstandsgate;
- een regressietest met 24 × 24 veelvuldig gelijke costs controleert
  determinisme en volledige matching.

In het finale replayartefact gebruikten 13 niet-lege overgangen de begrensde
exacte top-k-DP en 17 grotere overgangen de exacte Hongaarse one-bestsolver.
Alle 30 identiteitsaudits registreren
`stv3-exact-bounded-global-assignment-v2`.

## Wat deze wijziging bewust niet doet

- geen vaste maximumsnelheid invoeren;
- geen echte detectie, observatie of extreme kandidaat verwijderen;
- geen bui afkeuren;
- geen KMI-, RainViewer- of andere providerdata herinterpreteren;
- geen empirische kans claimen uit ongekalibreerde costverschillen;
- geen live engine, Home Assistant, worker of database wijzigen.

## Nog nodig voor meteorologisch gevalideerde identiteit

Een definitieve identiteitsclaim vereist minstens:

1. replay op exact opgeslagen native providerartefacten met tijd- en
   geolocatieprovenance;
2. voldoende object-motiondekking op native of wetenschappelijk gevalideerde
   analyseresolutie;
3. onafhankelijk geannoteerde split-, merge-, groei-, dissipatie- en
   verplaatsingsgebeurtenissen;
4. event-held-out kalibratie van associatiekosten en birth/death-priors;
5. evaluatie van meerdere hypotheses met onder meer IDF1/HOTA-achtige
   trackmetrics en betrouwbaarheidsintervallen;
6. regionale en provideroverschrijdende validatie zonder dezelfde radar als
   zowel voorspelling als waarheid te gebruiken.

Tot die gates voldaan zijn, is een gekozen lijn door opeenvolgende cellen een
auditbare hypothese en geen bewezen fysiek object.
