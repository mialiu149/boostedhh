from __future__ import annotations

import importlib.metadata

import boostedhh as m


def test_version():
    assert importlib.metadata.version("boostedhh") == m.__version__
