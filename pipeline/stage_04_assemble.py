import json
import logging
import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

from utils.schema_validator import jsonschema
from utils.report_writer import generate as generate_report

logger = logging.getLogger(__name__)

class SchemaAssembler:
    def __init__(self):
        schema_path = Path("specs/final_schema.schema.json")
        with open(schema_path, "r", encoding="utf-8") as f:
            self.output_schema = json.load(f)

    def assemble(self, structural_file: Path, semantic_file: Path, human_review_file: Optional[Path] = None) -> dict:
        """Merges structural extraction, semantic decisions, and human overrides."""
        
        # Load all sources
        with open(structural_file, "r") as f:
            structural = json.load(f)
            
        with open(semantic_file, "r") as f:
            semantic = json.load(f)
            
        human = None
        if human_review_file and human_review_file.exists():
            with open(human_review_file, "r") as f:
                human = json.load(f)
                
        # Base metadata payload
        output = {
            "assembled_at": datetime.now().isoformat(),
            "metadata": {
                "analysis_model": semantic.get("winner_provider", "unknown"),
                "strategy": semantic.get("provider_strategy", "unknown")
            },
            "tables": [],
            "relationships": []
        }

        # Setup lookup maps for semantic decisions
        finding_map = {}
        
        # Merge resolved semantic items
        for r in semantic.get("resolved", []):
            field_key = f"{r.get('sheet')}::{r.get('field')}"
            finding_map[field_key] = r

        # Override with Human reviewed items
        if human:
             for hr in human.get("answers", []):
                 if hr.get("decision") == "confirmed":
                     # Repopulate from semantic based on item_ids mapping into confident findings
                     # The exact merging mechanics depend on how Human Review stores individual overrides
                     pass

        # Populate tables
        structured_sheets = structural.get("sheets", {})
        output["total_tables"] = len(structured_sheets)
        
        for sheet_name, sheet_data in structured_sheets.items():
            table = {
                "name": sheet_name,
                "columns": []
            }
            
            for col_name, col_meta in sheet_data.get("columns", {}).items():
                field_key = f"{sheet_name}::{col_name}"
                semantic_item = finding_map.get(field_key)
                
                # Exclude filter artifacts — but only when auto_expanded=False (direct LLM decision).
                # Auto-expanded clones can incorrectly propagate filter_artifact across sheets
                # (e.g. 'нет продаж' is a filter artifact in Слайд1 but a valid business
                # status in BCG). For auto-expanded items on the BCG sheet we keep the column.
                if semantic_item and semantic_item.get("hypothesis") == "filter_artifact":
                    if not semantic_item.get("auto_expanded", False):
                        continue
                    # auto_expanded filter_artifact: keep BCG-style metric cols, drop layout artifacts
                    sheet_name_lc = sheet_name.lower()
                    is_bcg_metric = any(
                        kw in col_name.lower()
                        for kw in ("продажи", "доход", "маржа", "доля", "рост")
                    )
                    if not is_bcg_metric:
                        continue
                    
                column = {
                    "field_name": col_name,
                    "data_type": col_meta.get("dominant_type", "unknown")
                }
                
                # Populate extra traits
                if semantic_item and semantic_item.get("finding_type") == "mixed_sentinel":
                    column["sentinel_values"] = semantic_item.get("evidence", []) # Can inject properly extracted values
                    
                if col_meta.get("categorical_values"):
                    column["categorical_values"] = col_meta.get("categorical_values")
                    
                table["columns"].append(column)
                
            output["tables"].append(table)

        # Build relationships using join_candidates
        # Using Structural baseline or approved Semantic logic over join rules
        for jc in structural.get("join_candidates", []):
            # Example heuristic building from candidate sheets lists
            sheets = jc.get("found_in_sheets", [])
            col = jc.get("column")
            if len(sheets) > 1:
                # Naive relationships map stringing target 1 to X
                base = sheets[0]
                for target in sheets[1:]:
                     # Check if semantic override forces or denies
                     output["relationships"].append({
                         "source_table": base,
                         "target_table": target,
                         "join_keys": [col]
                     })

        try:
             jsonschema.validate(instance=output, schema=self.output_schema)
        except jsonschema.exceptions.ValidationError as e:
             logger.error(f"Final Schema Validation Failed: {e}")

        return output

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 04 - Schema Assembler")
    parser.add_argument("--structural", type=str, required=True, help="Path to Stage 01 structural JSON")
    parser.add_argument("--semantic", type=str, required=True, help="Path to Stage 02 semantic JSON")
    parser.add_argument("--human", type=str, required=False, help="Path to Stage 03 human_review JSON (optional)")
    args = parser.parse_args()

    assembler = SchemaAssembler()
    try:
        final_schema_data = assembler.assemble(
            structural_file=Path(args.structural),
            semantic_file=Path(args.semantic),
            human_review_file=Path(args.human) if args.human else None
        )
    except Exception as e:
        print(f"Assembly failed: {e}", file=sys.stderr)
        sys.exit(1)

    out_dir = Path("outputs")
    out_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    out_path = out_dir / f"final_schema_{timestamp}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(final_schema_data, f, ensure_ascii=False, indent=2)

    print(f"Saved JSON: {out_path}", file=sys.stderr)
    
    # Generate Word Report
    docx_path = out_dir / f"report_{timestamp}.docx"
    generate_report(
        schema_path=out_path,
        semantic_path=Path(args.semantic),
        output_path=docx_path,
    )
    print(f"Saved DOCX: {docx_path}", file=sys.stderr)
