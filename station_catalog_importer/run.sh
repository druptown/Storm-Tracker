#!/bin/sh
set -eu

exec python -m storm_tracker_worker.netatmo_world_importer "$@"
