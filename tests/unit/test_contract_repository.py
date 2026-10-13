from __future__ import annotations

import pytest
import yaml

from soda_lakehouse_dq.adapters.contracts import FileContractRepository
from soda_lakehouse_dq.domain.errors import ContractNotFoundError, ContractRenderError
from soda_lakehouse_dq.domain.models import Layer
from tests.conftest import PROJECT_ROOT


def _write(tmp_path, text, name="orders.yml"):
    folder = tmp_path / "contracts" / "retail" / "silver"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / name).write_text(text)
    return FileContractRepository(tmp_path / "contracts", tmp_path / "rendered", {"dataset_root": "ds"})


def test_renders_token_and_writes_file(tmp_path):
    repo = _write(tmp_path, 'dataset: "{{ dataset_root }}/silver/orders"\ncolumns: []\n')
    [spec] = repo.list_contracts("retail", Layer.SILVER)
    assert spec.dataset == "ds/silver/orders"
    assert spec.name == "retail.silver.orders"
    assert spec.rendered_path.read_text().startswith('dataset: "ds/silver/orders"')


def test_reference_datasets_are_collected(tmp_path):
    text = (
        'dataset: "{{ dataset_root }}/silver/orders"\ncolumns:\n  - name: c\n    checks:\n'
        "      - invalid:\n          valid_reference_data:\n"
        '            dataset: "{{ dataset_root }}/silver/customers"\n            column: id\n'
    )
    [spec] = _write(tmp_path, text).list_contracts("retail", Layer.SILVER)
    assert spec.references == ("ds/silver/customers",)
    assert spec.datasets == ("ds/silver/orders", "ds/silver/customers")


def test_soda_variables_are_left_alone(tmp_path):
    repo = _write(tmp_path, 'dataset: "{{dataset_root}}/s/t"\nfilter: "x > ${var.X}"\ncolumns: []\n')
    [spec] = repo.list_contracts("retail", Layer.SILVER)
    assert "${var.X}" in spec.rendered_path.read_text()


def test_missing_folder_raises(tmp_path):
    repo = FileContractRepository(tmp_path, tmp_path / "r", {})
    with pytest.raises(ContractNotFoundError):
        repo.list_contracts("retail", Layer.GOLD)


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ('dataset: "{{ nope }}/t"\ncolumns: []\n', "unknown token"),
        ("columns: []\n", "missing 'dataset'"),
        ("dataset: ds/s/t\n", "missing 'columns'"),
        ("- a\n- b\n", "mapping"),
        ("dataset: [unclosed\n", "invalid YAML"),
    ],
)
def test_invalid_contracts(tmp_path, text, message):
    with pytest.raises(ContractRenderError, match=message):
        _write(tmp_path, text).list_contracts("retail", Layer.SILVER)


@pytest.mark.parametrize("root", ["lakehouse_duckdb", "lakehouse_trino/lakehouse"])
def test_shipped_contracts_render_for_every_engine(tmp_path, root):
    repo = FileContractRepository(PROJECT_ROOT / "contracts", tmp_path, {"dataset_root": root})
    for layer in Layer.ordered():
        for spec in repo.list_contracts("retail", layer):
            document = yaml.safe_load(spec.rendered_path.read_text())
            assert document["dataset"].startswith(f"{root}/{layer.value}/")
            assert document["columns"], spec.name
            assert "{{" not in spec.rendered_path.read_text()
