"""`.env.example` documents every setting, so a newcomer can configure everything from it."""

from __future__ import annotations

import re

from soda_lakehouse_dq.settings import Settings
from tests.conftest import PROJECT_ROOT


def test_env_example_lists_every_setting() -> None:
    text = (PROJECT_ROOT / ".env.example").read_text()
    listed = set(re.findall(r"^#?\s*(DQ_[A-Z_]+)=", text, re.M))
    expected = {f"DQ_{name.upper()}" for name in Settings.model_fields}
    assert expected - listed == set(), "add these to .env.example"
    assert listed - expected == set(), "these are not settings"
