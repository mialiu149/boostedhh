"""
Checking new vs old xsecs. TODO: convert this to github action!
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

with Path("xsecs.json").open("rb") as f:
    xsecs = json.load(f)

with Path("xsecs_backup.json").open("rb") as f:
    xsecsb = json.load(f)

for k, v in xsecsb.items():
    if k not in xsecs:
        print(f"Missing {k}! {v}")
    elif not np.isclose(xsecs[k], v, rtol=1e-5):
        print(f"Discrepancy for {k}! {v} vs {xsecs[k]}")
