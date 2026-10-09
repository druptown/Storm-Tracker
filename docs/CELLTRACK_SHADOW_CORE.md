# CellTrack shadowkern

Status: lokaal onderzoeksfundament; niet live gekoppeld en niet deploybaar.

## Verticale API

1. `AnalysisGrid`
   - Projectie-ID, metrische origin, resoluties, dimensies en asrichtingen zijn
     expliciet.
   - De origin is de noordwestelijke buitenrand; pixelgrenzen zijn halfopen.
   - Pixel/positie-roundtrips, rastervorm en caller-supplied resolutielimieten
     kunnen fail-closed gecontroleerd worden.
   - Rasterresolutie wordt niet voorgesteld als meetnauwkeurigheid.

2. `NormalizedRasterFrame` en `detect_hierarchical_cells`
   - Accepteert alleen reeds geprojecteerde en fysisch genormaliseerde
     rasterwaarden.
   - Detecteert met connected components een rain shield en de daarin liggende
     embedded cores.
   - Shield/core-drempels, minimumoppervlakken, 4/8-connectiviteit,
     centroidmethode en beide positie-onzekerheden komen verplicht van de
     caller.
   - Minimumoppervlakken verwijderen geen component meer. Elk kleiner shield
     of core blijft als volledige detectie aanwezig met
     `low_confidence_fragment=true` en het exacte gebruikte minimumoppervlak.
   - Detectie-identiteit wordt uit frame, objectsoort, parent en footprint
     afgeleid; een nieuw los fragment hernummert bestaande objecten niet.
   - Footprintpixels, oppervlak, beide centroidvarianten, gemiddelde,
     maximum, threshold en parentrelatie blijven auditbaar.

3. `TrackMotionPrior`
   - Vanaf twee geassocieerde detecties is de prior exact de verplaatsing
     tussen de laatste twee posities gedeeld door hun tijdsinterval.
   - De statecovariantie komt uit beide onafhankelijke
     positiemeetcovarianties en expliciete caller-supplied procesruis.
   - Bij één detectie is uitsluitend een zero- of optical-flow-cold-start met
     caller-supplied brede 4x4-covariantie toegestaan.
   - Ook een extreme intervalvelocity wordt niet gecapt, gesmoothed of
     afgekeurd.

4. `OpticalFlowSample` en `predict_with_optical_flow`
   - Accepteert een lokale SI-snelheidsvector van een externe optical-flow
     schatter.
   - Meet-, gepropageerde flow- en gekalibreerde residualcovariantie blijven
     apart beschikbaar.
   - De som en positieve semidefiniteit worden gecontroleerd.
   - Er bestaat geen verborgen confidence, snelheidslimiet of clipping.

5. `estimate_lucas_kanade_flow`
   - Berekent een echte single-scale NumPy Lucas–Kanade-baseline uit twee
     compatibele `NormalizedRasterFrame`-objecten en hun werkelijke
     tijdsinterval.
   - Levert per pixel metrische `vx/vy`, structure-tensorconditionering,
     geometrische quality, brightness-residual en velocitycovariantie.
   - Nodata, onvoldoende window-support en het apertureproblem worden
     afzonderlijke expliciete statussen; er wordt geen nulbeweging verzonnen.
   - Window, sampleminimum, eigenwaarde/condition threshold, regularisatie,
     residualvloer en procesruis zijn caller-supplied.
   - `aggregate_detection_flow_sample` maakt alleen bij voldoende expliciete
     footprintdekking een `OpticalFlowSample`. Anders blijft de detectie
     bestaan met een unavailable-resultaat voor een brede cold start.
   - Resultaten hebben een deterministische checksum en een kleine
     collision-safe cache op frame-ID's, algoritme en configuratie.

6. `estimate_sparse_pyramidal_motion`
   - Pure-NumPy sparse challenger naast de dense single-scalebaseline:
     deterministische Shi–Tomasi-features, coarse-to-fine iteratieve
     Lucas–Kanade en objectaggregatie.
   - pySTEPS documenteert een vergelijkbare sparse route met OpenCV. Deze
     implementatie gebruikt bewust geen OpenCV of SciPy omdat de workerimage
     alleen NumPy garandeert; ze is niet dezelfde implementatie.
   - Ondersteunt twee of meer compatibele frames en maakt voor ieder
     opeenvolgend paar een resultaat met echte timestamps en metrische
     `vx/vy`.
   - Iedere geselecteerde feature blijft als immutable converged vector,
     iteration-limit vector, insufficient-support, ill-conditioned of
     numerical-failure record bestaan. Er is geen speed cap of stille
     outlierverwijdering.
   - Het caller-featurebudget selecteert deterministisch de beste
     Shi–Tomasi-kandidaten met een pyramideveilige marge en
     minimumafstand. Candidate-, margin-suppressed-, distance-suppressed-,
     budget-dropped- en selected-counts blijven auditbaar.
   - Runtime en geheugen zijn begrensd door callerlimieten plus harde maxima
     voor inputpixels, features, pyramideniveaus, window en iteraties. Alleen
     sparse resultaten worden bewaard; pyramidearrays niet.
   - Cellaggregatie kan caller-supplied conservatief gemiddeld of
     residualgewogen worden. Alle vectoren blijven aanwezig en bij weighting
     worden minimum/maximumgewicht gepubliceerd.
   - `SparseReplayFlowSampleFactory` past later op de bestaande replay
     flow-extension. Onbeschikbare flow gaat verplicht naar een
     caller-supplied brede cold-startfactory.
   - Resultaten hebben een deterministische checksum en collision-safe cache.

   Replaykoppeling blijft dependency-injection en wijzigt de bestaande replay
   niet:

   ```python
   sequence = estimate_sparse_pyramidal_motion(normalized_frames, flow_config)
   flow_factory = SparseReplayFlowSampleFactory(
       sequence=sequence,
       aggregation_config=cell_flow_config,
       unavailable_factory=caller_broad_cold_start_sample,
   )
   result = run_bounded_shadow_assignment(
       replay,
       flow_sample_factory=flow_factory,
       # overige caller-supplied costs en grenzen
   )
   ```

   `unavailable_factory` is verplicht. De sparse module kiest dus nooit zelf
   een nulvector, universele covariance of meteorologische fallback.

7. `build_association_feature_table`
   - Bouwt de volledige `previous × current`-kandidatenset.
   - Berekent optical-flow residual, innovatiecovariantie, Mahalanobisafstand,
     intervalspeed, area/intensityverschillen, footprint-IoU, providerhandoff
     en objectkind.
   - Een Gaussian NLL is uitsluitend een ongekalibreerd geometrisch kenmerk,
     geen kans en geen identiteitsbeslissing.

8. `GlobalAssignmentProblem` en globale solvers
   - Vereist voor iedere kandidaat een eindige caller-supplied cost.
   - Vereist expliciete birth- en deathcosts.
   - Geeft voor kleinere auditframes exacte globale top-k oplossingen tot een
     expliciete capaciteitsgrens.
   - Schakelt daarboven zonder detectionverlies over op een schaalbare exacte
     one-best min-cost-flowoplossing met formele birth/death-dummyedges.
   - De volledige edge-tabel en niet-winnende alternatieven blijven bewaard.
   - Bij capaciteitsoverschrijding faalt de solver; hij schakelt niet stil over
     naar een greedy benadering.

9. `CellTrackLedger`
   - Tracks en revisies zijn immutable.
   - Tentative, confirmed, coasting, ended en gaplimiet worden bepaald door
     expliciet lifecyclebeleid.
   - Assignment kan atomisch matches, misses en births toepassen.
   - Split en merge zijn expliciete lineage-events; beëindigde parents blijven
     in het ledger aanwezig.

De gedeelde shadowmodules staan byte-identiek onder:

- `custom_components/storm_tracker_v3/engine/`
- `storm_tracker_worker/tracking_core/engine/`

## Bewust nog niet geïmplementeerd

- Providerdecoding, reprojection, unitnormalisatie en exacte raw-byte lineage.
- Product/regime-specifieke residualkalibratie en multi-scale/iteratieve
  validatie. Dense Lucas–Kanade blijft de controleerbare single-scalebaseline;
  sparse pyramidal NumPy is de bounded challenger.
- Gekalibreerde featuregewichten, birth/deathpriors en kosten per weertype.
- Automatische inferentie van split/mergehypothesen uit een hogere-orde
  assignmentgraph; de lifecycle kan bewezen events al verliesloos vastleggen.
- Schaal- en duurmetingen van de exacte min-cost-flowsolver op realistische
  maximumframes; approximate beleid is nog niet nodig of gevalideerd.
- Polygonen/MultiPolygonen met gaten en geadvecteerde footprint-overlap.
- Persistente opslag van detecties, featuretabellen, assignments en ledgers.
- Echte replays van minimaal twintig frames, identity-ground-truth,
  meteorologische skillmetingen en NUC-soaktests.
- Live HA/worker wiring, startupbarrières en deployment.

Deze punten zijn releasegates. De shadowkern bewijst de contracten en
verwerkingslogica, niet de meteorologische skill op echte gebeurtenissen.
De sparse challenger krijgt pas een skillclaim na held-out hindcasts met
echte, onafhankelijke meteorologische events.
