FROM ghcr.io/home-assistant/base:latest

ARG BUILD_VERSION
ARG BUILD_ARCH

LABEL \
  io.hass.version="${BUILD_VERSION}" \
  io.hass.type="app" \
  io.hass.arch="${BUILD_ARCH}"

RUN apk add --no-cache cpulimit python3 py3-pip util-linux \
  && python3 -m venv /opt/storm-tracker \
  && /opt/storm-tracker/bin/pip install --no-cache-dir \
    paho-mqtt==2.1.0 numpy==2.5.1 h5py==3.16.0 pyproj==3.7.2 Pillow==11.3.0 \
    "psycopg[binary]==3.3.4"

COPY . /opt/storm_tracker_worker
COPY run.sh /run.sh
RUN chmod a+x /run.sh

ENV PYTHONPATH=/opt \
    STV3_CPU_THREADS=2 \
    OMP_NUM_THREADS=2 \
    OPENBLAS_NUM_THREADS=2 \
    MKL_NUM_THREADS=2 \
    NUMEXPR_NUM_THREADS=2
CMD ["/run.sh"]
