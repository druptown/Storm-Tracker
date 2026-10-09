FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/storm-tracker

WORKDIR /opt/storm-tracker

RUN python -m pip install --no-cache-dir \
    aiohttp==3.14.3 "psycopg[binary]==3.3.4"

COPY storm_tracker_worker /opt/storm-tracker/storm_tracker_worker
COPY station_catalog_importer/run.sh /usr/local/bin/storm-tracker-station-importer

RUN chmod 0755 /usr/local/bin/storm-tracker-station-importer \
    && mkdir -p /state \
    && chmod 0700 /state

VOLUME ["/state"]

ENTRYPOINT ["/usr/local/bin/storm-tracker-station-importer"]
