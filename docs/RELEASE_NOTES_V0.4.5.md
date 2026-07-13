# Storm Tracker V3 v0.4.5

## OPERA-detectiefix

Een echte bui binnen de ingestelde 200 km-radius leverde nul cellen op. Analyse
van het actuele OPERA-composiet wees uit dat echte neerslagpixels `qi_total=0.0`
konden hebben. De eerdere quality-eis van 0.5 verwijderde daardoor alle regen.

De quality-laag blijft beschikbaar als diagnostiek, maar is geen harde filter
meer. Kleine buien worden voortaan gedetecteerd vanaf 8 dBZ en 5 aaneengesloten
pixels (ongeveer 5 km²). Nodata en undetect blijven uitgesloten.
