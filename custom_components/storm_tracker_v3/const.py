"""Storm Tracker V3 — const.py v0.4.3"""

DOMAIN = "storm_tracker_v3"

# Providers
PROVIDER_BLITZORTUNG = "blitzortung"
PROVIDER_KMI         = "kmi"
PROVIDER_RAINVIEWER  = "rainviewer"
PROVIDER_NETATMO     = "netatmo"

# Tracker types
TRACKER_TYPE_HOME   = "home"
TRACKER_TYPE_PERSON = "person"

# Config keys
CONF_TRACKER_NAME      = "naam"
CONF_TRACKER_TYPE      = "type"
CONF_LOCATION_ENTITY   = "location_entity"
CONF_BLITZ_DISTANCE    = "blitz_distance_entity"
CONF_MAX_DISTANCE_KM   = "max_distance_km"
CONF_NOTIFY_SERVICE    = "notify_service"
CONF_NETATMO_CLIENT_ID = "netatmo_client_id"
CONF_NETATMO_SECRET    = "netatmo_client_secret"
CONF_NETATMO_TOKEN     = "netatmo_refresh_token"

# Defaults
DEFAULT_MAX_DISTANCE_KM   = 300
DEFAULT_CLUSTER_RADIUS_KM = 30
DEFAULT_EXPIRE_MINUTES    = 5
DEFAULT_MAX_STORMS        = 15
DEFAULT_BATCH_INTERVAL_S  = 1.0   # strikes 1s batchen voor één update

# KMI dekkingsgebied
KMI_LAT_MIN = 48.5
KMI_LAT_MAX = 52.0
KMI_LON_MIN = -1.5
KMI_LON_MAX =  9.5

# Geometry
EARTH_RADIUS_KM = 6371.0

# Storm statussen
STATUS_APPROACHING  = "Nadert"
STATUS_PASSING      = "Passeert"
STATUS_OVER_US      = "⚡ KOMT OVER ONS"
STATUS_MOVING_AWAY  = "Beweegt weg"
STATUS_STATIONARY   = "Stilstaand"
STATUS_INSUFFICIENT = "Onvoldoende data"

# Confidence
CONFIDENCE_HIGH   = "Hoog"
CONFIDENCE_MEDIUM = "Matig"
CONFIDENCE_LOW    = "Laag"
CONFIDENCE_NONE   = "Onvoldoende data"
