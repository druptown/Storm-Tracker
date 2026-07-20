"""Inspecteer de publieke DWD RV LATEST-container zonder bestanden te bewaren."""
from __future__ import annotations

import io
import tarfile
import urllib.request

import h5py

URL = "https://opendata.dwd.de/weather/radar/composite/rv/composite_rv_LATEST.tar"

body = urllib.request.urlopen(URL, timeout=30).read()
with tarfile.open(fileobj=io.BytesIO(body)) as archive:
    members = archive.getmembers()
    print("bytes", len(body))
    print("members", [(item.name, item.size) for item in members[:8]])
    stream = archive.extractfile(members[0])
    payload = stream.read()
    print("header", repr(payload[:16]))
    with h5py.File(io.BytesIO(payload), "r") as dataset:
        print("root_attrs", dict(dataset.attrs))
        dataset.visititems(lambda name, obj: print(
            name, getattr(obj, "shape", None), dict(obj.attrs)
        ))
