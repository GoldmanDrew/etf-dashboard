"""Bucket 2 UI visibility — YieldBOOST-only tab, full data retained."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from build_data import bucket_2_ui_visible_for_row  # noqa: E402


class TestBucket2UiVisibility:
    def test_passive_low_beta_hidden_from_b2_tab(self):
        assert bucket_2_ui_visible_for_row(
            "SSO",
            "bucket_2_low_beta",
            is_yieldboost=False,
            product_class="passive_low_delta",
        ) is False

    def test_yieldboost_visible_in_b2(self):
        assert bucket_2_ui_visible_for_row(
            "NVYY",
            "bucket_2_low_beta",
            is_yieldboost=True,
            product_class="income_yieldboost",
        ) is True

    def test_fof_visible_in_b2(self):
        assert bucket_2_ui_visible_for_row(
            "YBTY",
            "bucket_2_low_beta",
            is_yieldboost=False,
            product_class="income_yieldboost_fof",
        ) is True

    def test_high_beta_not_applicable(self):
        assert bucket_2_ui_visible_for_row(
            "TQQQ",
            "bucket_1_high_beta",
            is_yieldboost=False,
            product_class="letf",
        ) is True

    def test_inverse_not_applicable(self):
        assert bucket_2_ui_visible_for_row(
            "SQQQ",
            "bucket_3_inverse",
            is_yieldboost=False,
            product_class="inverse",
        ) is True

    @pytest.mark.parametrize(
        "sym",
        ["AAPW", "CONY", "AMZY", "GDXW"],
    )
    def test_sample_passive_low_delta_hidden(self, sym):
        assert bucket_2_ui_visible_for_row(
            sym,
            "bucket_2_low_beta",
            is_yieldboost=False,
            product_class="passive_low_delta",
        ) is False
