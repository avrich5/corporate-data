import json
from pathlib import Path

import pytest
import subprocess

from pipeline.stage_01_extract import extract
from utils.schema_validator import validate_structural_output


@pytest.fixture(scope="module")
def extracted_report():
    fixture_path = Path("tests/fixtures/bcg_du_2025_q4.xlsx")
    assert fixture_path.exists(), "Test fixture not found! Ensure it was copied."
    return extract(fixture_path)


def test_header_detection(extracted_report):
    sheets = extracted_report["sheets"]
    assert sheets["Слайд 1 тренды категорий (2)"]["header_row_index"] == 1
    assert sheets["Слайд 1 тренды категорий"]["header_row_index"] == 0
    assert sheets["Тренды категорий"]["header_row_index"] == 0
    assert sheets["BCG"]["header_row_index"] == 4
    assert sheets["ABC расчет"]["header_row_index"] == 0
    assert sheets["Факт продаж"]["header_row_index"] == 9
    assert sheets["План Антонова"]["header_row_index"] == 4

def test_mixed_type_columns(extracted_report):
    bcg = extracted_report["sheets"]["BCG"]
    
    mixed_cols = []
    for col, meta in bcg["columns"].items():
        if meta["mixed"]:
            mixed_cols.append(col)
            # All mixed columns must have the specific sentinel list
            assert meta["mixed_sentinel_values"] == ["нет продаж"]
            
    assert len(mixed_cols) == 16, f"Expected 16 mixed columns but found {len(mixed_cols)}"
    
    # "КАТЕГОРИЯ" should not be mixed
    assert not bcg["columns"]["КАТЕГОРИЯ"]["mixed"]


def test_join_candidates(extracted_report):
    candidates = extracted_report["join_candidates"]
    
    col_names = [c["column"] for c in candidates]
    
    assert "категория" in col_names
    assert "линия модели" in col_names
    
    for name in col_names:
        assert not name.startswith("_unnamed")
        
    assert len(candidates) <= 5


def test_horizontal_layout(extracted_report):
    sheets = extracted_report["sheets"]
    
    assert sheets["BCG"]["layout_type"] == "horizontal_periods"
    assert sheets["BCG"]["period_count"] == 4
    
    assert sheets["ABC расчет"]["layout_type"] == "horizontal_periods"
    assert sheets["Факт продаж"]["layout_type"] == "horizontal_periods"
    assert sheets["Тренды категорий"]["layout_type"] == "standard"


def test_anomaly_precision(extracted_report):
    slide_2 = extracted_report["sheets"]["Слайд 1 тренды категорий (2)"]
    anomaly_count = len(slide_2["anomaly_rows"])
    
    # Must be under 20% of 61 rows (<= 12)
    assert anomaly_count < 12, f"Anomaly false-positive rate too high: {anomaly_count}"


def test_output_schema(extracted_report):
    # This just ensures our validation passed successfully inside extract()
    # But let's re-run it directly to be sure and prove it handles pure dict.
    validate_structural_output(extracted_report)


def test_isolation():
    fixture_path = Path("tests/fixtures/bcg_du_2025_q4.xlsx").absolute()
    # Script should run explicitly via CLI module invocation
    result = subprocess.run(
        ["python", "-m", "pipeline.stage_01_extract", "--input", str(fixture_path)],
        capture_output=True,
        text=True,
        cwd=Path(".").absolute()
    )
    assert result.returncode == 0, f"CLI runner failed: {result.stderr}"
    assert "Successfully extracted" in result.stderr or "Successfully extracted" in result.stdout
