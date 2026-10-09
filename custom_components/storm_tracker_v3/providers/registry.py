"""Authoritative registry of operational radar sources.

The provider policy is validated against this registry so dashboard policy can
never advertise a parser that is not present in this release.
"""

IMPLEMENTED_RADAR_PROVIDERS = frozenset({
    "kmi",
    "knmi",
    "dwd_radolan",
    "meteofrance_radar",
    "dpc_radar",
    "aemet_radar",
    "met_office_radar",
    "opera",
    "rainviewer",
    "noaa_enterprise_rainrate",
    "noaa_goes_rrqpe",
    "hsaf_h40b",
})

# Netatmo is fetched through its integration provider. Synoptic and METAR
# deliberately run in the worker, but they are equally real operational
# ground sources and must therefore be represented in the capability policy.
IMPLEMENTED_GROUND_PROVIDERS = frozenset({
    "netatmo",
    "synoptic",
    "metar",
})
