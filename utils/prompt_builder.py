import json
import logging
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger(__name__)

LLM_MAX_CONTEXT_TOKENS = 8000
MAX_MIXED_SENTINEL_ITEMS = 10   # cap to avoid response truncation
MAX_EMPTY_COLUMN_ITEMS   =  5


class PromptBuilder:
    def __init__(self, templates_dir: Path):
        self.env = Environment(loader=FileSystemLoader(str(templates_dir)))
        self.template = self.env.get_template("semantic_analyzer.jinja2")

    def build_payload(self, structural_data: dict) -> dict:
        """Parses Stage 01 output and transforms it into template context variables."""
        findings = {
            "mixed_sentinel": [],
            "join_candidate": [],
            "layout_interpretation": [],
            "anomaly_row_intent": [],
            "empty_column_intent": [],
            "sheet_role": []
        }

        # 1. Join Candidates
        for jc in structural_data.get("join_candidates", []):
            findings["join_candidate"].append({
                "field": jc["column"],
                "overlap": jc.get("unique_value_overlap", 0.0),
                "sheets": jc.get("found_in_sheets", [])
            })

        # 2. Iterate Sheets for Sheet Roles and Column/Row findings
        for sheet_name, sheet_data in structural_data.get("sheets", {}).items():
            layout_type = sheet_data.get("layout_type", "standard")
            
            # Context Truncation Guard: Skip presentation sheets if we are worried about size,
            # but per spec: "Never silently drop findings". Here we just include them all
            # and rely on the model for now, unless tokens explode.
            
            # Sheet Role — truncate column list to avoid bloating the prompt
            col_names = list(sheet_data.get("columns", {}).keys())
            findings["sheet_role"].append({
                "sheet": sheet_name,
                "layout_type": layout_type,
                "columns": col_names[:20],
                "total_columns": len(col_names)
            })

            # Horizontal Layouts
            if layout_type == "horizontal_periods":
                 findings["layout_interpretation"].append({
                     "sheet": sheet_name,
                     "period_count": sheet_data.get("period_count"),
                     "period_labels": sheet_data.get("period_labels", [])
                 })

            # Anomaly Rows
            for anomaly in sheet_data.get("anomaly_rows", []):
                if anomaly.get("reason") == "keyword":
                     findings["anomaly_row_intent"].append({
                         "sheet": sheet_name,
                         "row_index": anomaly.get("row_index"),
                         "content": anomaly.get("value")
                     })

            # Column Iteration (Mixed Sentinels & Empty Intents)
            for col_name, col_meta in sheet_data.get("columns", {}).items():
                if col_meta.get("mixed") and col_meta.get("mixed_sentinel_values"):
                    if len(findings["mixed_sentinel"]) < MAX_MIXED_SENTINEL_ITEMS:
                        findings["mixed_sentinel"].append({
                            "sheet": sheet_name,
                            "field": col_name,
                            "sentinels": col_meta["mixed_sentinel_values"],
                            "sample_values": col_meta.get("sample_values", [])[:3],
                            "categorical_values": col_meta.get("categorical_values", [])[:5],
                            "layout_type": layout_type
                        })
                    else:
                        # Log a summary entry instead of full detail
                        logger.info(
                            f"mixed_sentinel cap reached — skipping detail for "
                            f"{sheet_name}::{col_name} (same pattern, same sheet)"
                        )

                if col_meta.get("dominant_type") == "empty":
                    if len(findings["empty_column_intent"]) < MAX_EMPTY_COLUMN_ITEMS:
                        findings["empty_column_intent"].append({
                            "sheet": sheet_name,
                            "field": col_name
                        })

        context = {
            "source_file": structural_data.get("source_file", "unknown"),
            "sheet_count": structural_data.get("sheet_count", 0),
            "sheet_names": structural_data.get("sheet_names", []),
            "findings": findings
        }
        
        # Render prompt string and detect system block split
        rendered = self.template.render(**context)
        return rendered
