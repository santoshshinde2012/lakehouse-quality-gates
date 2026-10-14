from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
import yaml

from soda_lakehouse_dq.adapters.contracts import FileContractRepository
from soda_lakehouse_dq.domain.errors import ContractNotFoundError, ContractRenderError, DataQualityError
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


def test_rendered_specs_are_isolated_by_engine_and_reuse_identical_content(tmp_path):
    duckdb = _write(tmp_path, 'dataset: "{{ dataset_root }}/silver/orders"\ncolumns: []\n')
    trino = FileContractRepository(
        tmp_path / "contracts", tmp_path / "rendered", {"dataset_root": "lakehouse_trino/lakehouse"}
    )
    [duckdb_spec] = duckdb.list_contracts("retail", Layer.SILVER)
    original = duckdb_spec.rendered_path.read_text()
    [trino_spec] = trino.list_contracts("retail", Layer.SILVER)
    [repeated_spec] = duckdb.list_contracts("retail", Layer.SILVER)
    assert duckdb_spec.rendered_path != trino_spec.rendered_path
    assert repeated_spec.rendered_path == duckdb_spec.rendered_path
    assert duckdb_spec.rendered_path.read_text() == original
    assert yaml.safe_load(trino_spec.rendered_path.read_text())["dataset"] == trino_spec.dataset


def test_source_changes_preserve_previously_rendered_specs(tmp_path):
    repo = _write(tmp_path, "dataset: ds/silver/orders\ncolumns: []\n")
    [first] = repo.list_contracts("retail", Layer.SILVER)
    original = first.rendered_path.read_text()
    first.source_path.write_text("dataset: ds/silver/orders\ncolumns: []\nchecks:\n  - row_count:\n")
    [changed] = repo.list_contracts("retail", Layer.SILVER)
    assert changed.rendered_path != first.rendered_path
    assert first.rendered_path.read_text() == original
    first.source_path.write_text(original)
    [restored] = repo.list_contracts("retail", Layer.SILVER)
    assert restored.rendered_path == first.rendered_path


def test_concurrent_renderers_return_complete_isolated_contracts(tmp_path):
    text = 'dataset: "{{ dataset_root }}/silver/orders"\ncolumns: []\n# ' + "comment " * 8192 + "\n"
    duckdb = _write(tmp_path, text)
    trino = FileContractRepository(
        tmp_path / "contracts", tmp_path / "rendered", {"dataset_root": "lakehouse_trino/lakehouse"}
    )
    repositories = [duckdb, trino] * 4
    barrier = Barrier(len(repositories))

    def render(repo):
        barrier.wait(timeout=10)
        paths = set()
        for _ in range(8):
            [spec] = repo.list_contracts("retail", Layer.SILVER)
            assert spec.rendered_path.read_text() == repo.render_text(text, spec.source_path)
            paths.add(spec.rendered_path)
        return paths

    with ThreadPoolExecutor(max_workers=len(repositories)) as executor:
        paths = set().union(*executor.map(render, repositories))
    assert len(paths) == 2
    assert not list((tmp_path / "rendered").rglob("*.tmp"))


def test_atomic_replacement_failure_preserves_existing_contract_and_cleans_temporary_file(
    tmp_path, monkeypatch
):
    repo = _write(tmp_path, "dataset: ds/silver/orders\ncolumns: []\n")
    [spec] = repo.list_contracts("retail", Layer.SILVER)
    original = spec.rendered_path.read_text()

    def fail_replace(source, destination):
        assert source.read_text() == original
        assert destination == spec.rendered_path
        assert destination.read_text() == original
        raise OSError("replacement failed")

    monkeypatch.setattr("soda_lakehouse_dq.adapters.contracts.os.replace", fail_replace)
    with pytest.raises(OSError, match="replacement failed"):
        repo.list_contracts("retail", Layer.SILVER)
    assert spec.rendered_path.read_text() == original
    assert not list((tmp_path / "rendered").rglob("*.tmp"))


def test_yaml_source_is_rendered_with_a_yml_suffix(tmp_path):
    [spec] = _write(tmp_path, "dataset: ds/silver/orders\ncolumns: []\n", name="orders.yaml").list_contracts(
        "retail", Layer.SILVER
    )
    assert spec.rendered_path.name == "orders.yml"


def test_reference_datasets_are_collected(tmp_path):
    text = (
        'dataset: "{{ dataset_root }}/silver/orders"\ncolumns:\n  - name: c\n    checks:\n'
        "      - invalid:\n          valid_reference_data:\n"
        '            dataset: "{{ dataset_root }}/silver/customers"\n            column: id\n'
    )
    [spec] = _write(tmp_path, text).list_contracts("retail", Layer.SILVER)
    assert spec.references == ("ds/silver/customers",)
    assert spec.datasets == ("ds/silver/orders", "ds/silver/customers")


@pytest.mark.parametrize("root", ["lakehouse_duckdb", "lakehouse_trino/lakehouse"])
def test_sql_read_dependencies_are_rendered_and_merged_with_reference_checks(tmp_path, root):
    text = (
        'dataset: "{{ dataset_root }}/silver/orders"\n'
        'attributes:\n  read_dependencies:\n    - "{{ dataset_root }}/bronze/orders"\n'
        '    - "{{ dataset_root }}/silver/customers"\n'
        '    - "{{ dataset_root }}/bronze/orders"\n'
        "columns:\n  - name: customer_id\n    checks:\n      - invalid:\n"
        "          valid_reference_data:\n"
        '            dataset: "{{ dataset_root }}/silver/customers"\n            column: id\n'
    )
    _write(tmp_path, text)
    repo = FileContractRepository(tmp_path / "contracts", tmp_path / "rendered", {"dataset_root": root})
    [spec] = repo.list_contracts("retail", Layer.SILVER)
    assert spec.references == (f"{root}/silver/customers", f"{root}/bronze/orders")
    assert spec.datasets == (f"{root}/silver/orders", *spec.references)
    assert yaml.safe_load(spec.rendered_path.read_text())["attributes"]["read_dependencies"] == [
        f"{root}/bronze/orders",
        f"{root}/silver/customers",
        f"{root}/bronze/orders",
    ]


@pytest.mark.parametrize(
    ("declaration", "message"),
    [
        (None, "must be a list"),
        ("ds/silver/orders", "must be a list"),
        ({"dataset": "ds/silver/orders"}, "must be a list"),
        ([1], "must be dataset strings"),
        ([""], "same dataset root"),
        (["other/silver/orders"], "same dataset root"),
        (["ds/other_catalog/silver/orders"], "same dataset root"),
        (["ds/silver"], "same dataset root"),
        (["ds/../orders"], "invalid schema or table"),
        (["ds/silver/orders; DROP TABLE orders"], "invalid schema or table"),
    ],
)
def test_invalid_sql_read_dependencies_fail_before_rendering(tmp_path, declaration, message):
    text = yaml.safe_dump(
        {"dataset": "ds/silver/orders", "columns": [], "attributes": {"read_dependencies": declaration}}
    )
    repo = _write(tmp_path, text)
    with pytest.raises(ContractRenderError, match=message):
        repo.list_contracts("retail", Layer.SILVER)
    assert not (tmp_path / "rendered").exists()


def test_sql_read_dependency_cannot_escape_the_trino_catalog(tmp_path):
    text = yaml.safe_dump(
        {
            "dataset": "ds/lakehouse/gold/metrics",
            "columns": [],
            "attributes": {"read_dependencies": ["ds/other_catalog/silver/orders"]},
        }
    )
    with pytest.raises(ContractRenderError, match="same dataset root"):
        _write(tmp_path, text).list_contracts("retail", Layer.SILVER)


def test_soda_variables_are_left_alone(tmp_path):
    repo = _write(tmp_path, 'dataset: "{{dataset_root}}/s/t"\nfilter: "x > ${var.X}"\ncolumns: []\n')
    [spec] = repo.list_contracts("retail", Layer.SILVER)
    assert "${var.X}" in spec.rendered_path.read_text()


def test_missing_folder_raises(tmp_path):
    repo = FileContractRepository(tmp_path, tmp_path / "r", {})
    with pytest.raises(ContractNotFoundError):
        repo.list_contracts("retail", Layer.GOLD)


@pytest.mark.parametrize("pipeline", ["../external", "..\\external", "..", ".", ""])
def test_pipeline_must_not_escape_the_contract_or_render_roots(tmp_path, pipeline):
    repo = _write(tmp_path, "dataset: ds/silver/orders\ncolumns: []\n")
    with pytest.raises(DataQualityError, match="pipeline must be a single non-empty path component"):
        repo.list_contracts(pipeline, Layer.SILVER)
    assert not (tmp_path / "rendered").exists()


def test_absolute_pipeline_is_rejected_before_reading_or_rendering_external_contracts(tmp_path):
    outside = tmp_path / "external" / "silver"
    outside.mkdir(parents=True)
    (outside / "orders.yml").write_text("dataset: ds/silver/orders\ncolumns: []\n")
    repo = FileContractRepository(tmp_path / "contracts", tmp_path / "rendered", {})
    with pytest.raises(DataQualityError, match="pipeline must be a single non-empty path component"):
        repo.list_contracts(str(outside.parent), Layer.SILVER)
    assert [path.name for path in outside.iterdir()] == ["orders.yml"]
    assert not (tmp_path / "rendered").exists()


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
            if layer is Layer.GOLD:
                assert spec.references == (f"{root}/silver/orders",)
