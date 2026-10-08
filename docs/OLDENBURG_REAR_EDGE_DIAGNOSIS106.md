# Oldenburg early-exit diagnosis

Read-only investigation requested by the user after the completed runtime106 replay.
The full report is retained at
`/home/ikke/storm-tracker/.analysis/oldenburg-replay-20261008/rear-edge-diagnosis106/report.txt`.
No production, database, provider or tracker changes. One 33-field projection and
two fixed NCC windows, with one cheap subpixel refinement; no optical-flow replay
or parameter search. Seven targeted helper tests pass.

The 01:49:33 issue's 03:47:03 exit is a >=0.1 mm/h threshold crossing from 0.12 to
0.03, not the complete dry boundary. Its original >0 interval remains horizon-
censored. Changing only motion from the original past 01:35/01:40 pair, with the
same full 01:40 QPE and two-hour horizon, yields censored rather than closed exit.

Retrospective measured-motion controls find enough rain already present in the
original field to reach the target through 05:20. Subpixel controls at 05:20 give
0.54/0.69 mm/h from the initial field versus later actual radar 0.24. At 05:40,
the unchanged initial field still supplies rain while the actual pixel is dry.
Therefore massive new growth is not required to explain the long duration, and
development includes decay as well. Later motion is diagnostic only, never a
new causal forecast score.

Local archived LK feature vectors agree with some local patch movements, whereas
larger-field motion differs. Evidence supports a scale/motion-persistence and
rain-episode semantics issue, not a blanket claim that all local vectors or the
decoder are wrong. Residual growth/decay mixes physical evolution, deformation and
registration uncertainty; exact attribution remains unavailable. No calibrated
accuracy, global-vector production recommendation or exact ground-truth claim.

Next direction: distinguish policy wet-window ends from episode ends and qualify
coherent contour-level movement separately from intensity kernels, with explicit
growth/decay uncertainty. Implementation requires a separate approved change.
