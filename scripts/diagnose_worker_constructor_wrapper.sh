#!/usr/bin/with-contenv bashio
set -e

exec() {
  /opt/storm-tracker/bin/python /diagnose_worker_constructor.py
}
export -f exec

source /run.sh
