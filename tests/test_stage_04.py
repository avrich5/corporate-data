import pytest
import json
from pathlib import Path
from unittest.mock import patch, mock_open
from pipeline.stage_04_assemble import SchemaAssembler

@pytest.fixture
def assembler():
    return SchemaAssembler()

@pytest.fixture
def mock_structural():
    return {
        "sheets": {
            "BCG": {
                "columns": {
                    "Доход": {"dominant_type": "number"},
                    "Status": {"dominant_type": "string"},
                    "Trash": {"dominant_type": "string"}
                }
            },
            "Other": {
                "columns": {
                    "Product": {"dominant_type": "string", "categorical_values": ["A", "B"]}
                }
            }
        },
        "join_candidates": []
    }

@pytest.fixture
def mock_semantic():
    return {
        "winner_provider": "openai",
        "provider_strategy": "compete",
        "resolved": [
            {
                "sheet": "BCG",
                "field": "Status",
                "finding_type": "mixed_sentinel",
                "hypothesis": "no_sales_status",
                "evidence": ["нет продаж"]
            },
            {
                "sheet": "BCG",
                "field": "Trash",
                "finding_type": "mixed_sentinel",
                "hypothesis": "filter_artifact"
            }
        ]
    }

def test_schema_assembly_excludes_filter_artifact(assembler, mock_structural, mock_semantic, tmp_path):
    struct_path = tmp_path / "struct.json"
    sem_path = tmp_path / "sem.json"
    
    with open(struct_path, "w") as f:
        json.dump(mock_structural, f)
    with open(sem_path, "w") as f:
        json.dump(mock_semantic, f)
        
    result = assembler.assemble(struct_path, sem_path)
    
    # Assert Trash is dropped due to "filter_artifact" hypothesis mapping
    bcg_table = next(t for t in result["tables"] if t["name"] == "BCG")
    assert len(bcg_table["columns"]) == 2
    column_names = [c["field_name"] for c in bcg_table["columns"]]
    assert "Доход" in column_names
    assert "Status" in column_names
    assert "Trash" not in column_names

def test_schema_assembly_injects_sentinels(assembler, mock_structural, mock_semantic, tmp_path):
    struct_path = tmp_path / "struct.json"
    sem_path = tmp_path / "sem.json"
    
    with open(struct_path, "w") as f:
        json.dump(mock_structural, f)
    with open(sem_path, "w") as f:
        json.dump(mock_semantic, f)
        
    result = assembler.assemble(struct_path, sem_path)
    
    bcg_table = next(t for t in result["tables"] if t["name"] == "BCG")
    status_col = next(c for c in bcg_table["columns"] if c["field_name"] == "Status")
    
    # Assert Semantic overrides are attached to structural baselines
    assert status_col["sentinel_values"] == ["нет продаж"]

def test_schema_assembly_metadata_passthrough(assembler, mock_structural, mock_semantic, tmp_path):
    struct_path = tmp_path / "struct.json"
    sem_path = tmp_path / "sem.json"
    
    with open(struct_path, "w") as f:
        json.dump(mock_structural, f)
    with open(sem_path, "w") as f:
        json.dump(mock_semantic, f)
        
    result = assembler.assemble(struct_path, sem_path)
    
    assert result["metadata"]["analysis_model"] == "openai"
    assert result["metadata"]["strategy"] == "compete"
    assert result["total_tables"] == 2
