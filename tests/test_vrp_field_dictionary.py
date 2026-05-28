"""Tests for scripts/vrp_field_dictionary.py."""

from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from vrp_field_dictionary import (  # noqa: E402
    FIELDS,
    VOL_VRP_ROW_KEYS,
    YB_EDGE_COLUMN_KEYS,
    all_required_field_keys,
    build_vrp_field_dictionary_payload,
    write_vrp_field_dictionary,
)


def test_every_required_key_has_tooltip():
    required = all_required_field_keys()
    assert required.issubset(FIELDS.keys())
    for key in required:
        entry = FIELDS[key]
        assert entry.get("label"), f"missing label for {key}"
        assert entry.get("tooltip"), f"missing tooltip for {key}"


def test_no_public_bs_field_keys():
    for key in FIELDS:
        assert not key.startswith("bs_"), f"public field key must not start with bs_: {key}"


def test_build_payload_shape():
    payload = build_vrp_field_dictionary_payload()
    assert payload["schema_version"] == 1
    assert payload["build_time"]
    assert isinstance(payload["fields"], dict)
    assert len(payload["fields"]) >= len(all_required_field_keys())


def test_write_vrp_field_dictionary(tmp_path):
    out = tmp_path / "vrp_field_dictionary.json"
    payload = write_vrp_field_dictionary(out)
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded == payload
    assert loaded["fields"]["edge_pp_of_max_loss"]["tooltip"]


def test_yb_edge_and_vol_vrp_key_lists_complete():
    assert len(YB_EDGE_COLUMN_KEYS) == 15
    assert len(VOL_VRP_ROW_KEYS) >= 20
