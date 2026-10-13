from __future__ import annotations

import pytest

from soda_lakehouse_dq.domain.models import Layer, coerce_variable


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("48", 48), ("2.5", 2.5), ("-1", -1), ("2024-03-01", "2024-03-01"), ("abc", "abc"), ("", "")],
)
def test_coerce_variable(raw, expected):
    value = coerce_variable(raw)
    assert value == expected
    assert type(value) is type(expected)


def test_layer_order():
    assert [layer.value for layer in Layer.ordered()] == ["bronze", "silver", "gold"]
