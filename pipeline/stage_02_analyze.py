import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

from utils.schema_validator import jsonschema
from utils.llm_client import AnthropicClient, OpenAIClient, LLMError
from utils.prompt_builder import PromptBuilder

logger = logging.getLogger(__name__)

# Config from spec
CONFIDENCE_RESOLVED = 0.85
CONFIDENCE_CONFIRM = 0.50

class SemanticAnalyzer:
    def __init__(self, anthropic_key: str, openai_key: str):
        self.anthropic_client = AnthropicClient(api_key=anthropic_key)
        self.openai_client = OpenAIClient(api_key=openai_key)
        self.prompt_builder = PromptBuilder(templates_dir=Path("prompts"))
        
        # Load schema for validation
        schema_path = Path("specs/semantic_output.schema.json")
        with open(schema_path, "r", encoding="utf-8") as f:
            self.output_schema = json.load(f)

    def _route_finding(self, finding: dict) -> tuple[str, dict]:
        """Applies routing logic based on confidence thresholds and overrides."""
        # 1. mixed_sentinel "План Антонова" override
        if finding.get("finding_type") == "mixed_sentinel" and finding.get("sheet") == "План Антонова":
            field_name = finding.get("field", "")
            if "All" in field_name or "_unnamed" in field_name:
                finding["hypothesis"] = "filter_artifact"
                finding["confidence"] = 0.95
                return "resolved", finding

        # 2. Unknown ALWAYS escalates (unless overridden by above)
        if finding.get("hypothesis") == "unknown":
            finding["question_for_human"] = f"Перевірте це поле. ШІ не зміг класифікувати його автоматично."
            return "escalate_queue", finding
            
        # 3. join_candidate with overlap == 0.0 ALWAYS confirms

        # 4. Standard threshold routing
        conf = finding.get("confidence", 0.0)
        if conf >= CONFIDENCE_RESOLVED:
            return "resolved", finding
        elif conf >= CONFIDENCE_CONFIRM:
            finding["question_for_human"] = "Впевненість середня. Підтвердіть або уточніть."
            return "confirm_queue", finding
        else:
            finding["question_for_human"] = "Впевненість низька. Потрібна ручна перевірка."
            return "escalate_queue", finding

    @staticmethod
    def _clean_llm_output(raw: str) -> str:
        """Strip markdown fences and isolate the JSON object."""
        s = raw.strip()
        # strip ```json ... ``` or ``` ... ```
        if "```" in s:
            parts = s.split("```")
            # parts[1] is the block content (may start with 'json\n')
            for part in parts:
                candidate = part.strip()
                if candidate.lower().startswith("json"):
                    candidate = candidate[4:].strip()
                if candidate.startswith("{"):
                    return candidate
        # No fences — find first { to last }
        start = s.find("{")
        end   = s.rfind("}")
        if start != -1 and end != -1:
            return s[start:end + 1]
        return s

    def _process_llm_json(self, raw_json_str: str, provider: str, model: str) -> dict:
        """Parses the LLM output and routes findings."""
        try:
            raw_json_str = self._clean_llm_output(raw_json_str)
            data = json.loads(raw_json_str)
            
            processed = {
                "resolved": [],
                "confirm_queue": [],
                "escalate_queue": [],
                "confidence_sum": 0.0
            }

            # Findings can be in resolved/confirm/escalate keys from the LLM, 
            # or we re-route them purely based on the LLM's returned objects
            all_findings = []
            for k in ["resolved", "confirm_queue", "escalate_queue"]:
                 all_findings.extend(data.get(k, []))

            for item in all_findings:
                item["provider"] = provider
                item["model"] = model
                
                queue_name, routed_item = self._route_finding(item)
                processed[queue_name].append(routed_item)
                
                if queue_name == "resolved":
                    processed["confidence_sum"] += routed_item.get("confidence", 0.0)

            return processed
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode LLM JSON from {provider}: {e}")
            raise LLMError(f"Invalid JSON from model: {e}")

    async def _run_strategy(self, user_prompt: str, strategy: str) -> tuple[dict, Optional[dict]]:
        """Executes the chosen strategy and returns (final_data, compete_log)."""
        system_prompt = "You are a data schema analyst. Always respond in the same language as the column names and data values you are analyzing."
        # The prompt_builder actually injected this into a single payload in our current template, 
        # so we will use the combined text as user_prompt, and system as essentially empty or repeating.
        
        comps = []
        if strategy in ("compete", "single_anthropic"):
            comps.append(self.anthropic_client.complete(user_prompt, system_prompt))
        if strategy in ("compete", "single_openai"):
            comps.append(self.openai_client.complete(user_prompt, system_prompt))

        results = await asyncio.gather(*comps, return_exceptions=True)
        
        valid_results = {}
        # Map back to provider based on strategy order
        idx = 0
        if strategy in ("compete", "single_anthropic"):
            res = results[idx]
            if not isinstance(res, Exception):
                valid_results["anthropic"] = res
            else:
                logger.warning(f"Anthropic failed: {res}")
            idx += 1
            
        if strategy in ("compete", "single_openai"):
            res = results[idx]
            if not isinstance(res, Exception):
                valid_results["openai"] = res
            else:
                logger.warning(f"OpenAI failed: {res}")

        if not valid_results:
            raise LLMError("All providers failed to return a valid response.")

        # Process responses
        parsed = {}
        for prov, resp in valid_results.items():
            parsed[prov] = self._process_llm_json(resp.content, resp.provider, resp.model)
            parsed[prov]["hash"] = resp.input_hash

        if strategy == "compete" and len(parsed) == 2:
            a_score = parsed["anthropic"]["confidence_sum"]
            o_score = parsed["openai"]["confidence_sum"]
            winner = "anthropic" if a_score >= o_score else "openai"
            
            compete_log = {
                "anthropic_confidence_sum": round(a_score, 2),
                "openai_confidence_sum": round(o_score, 2),
                "winner": winner,
                "anthropic_raw_response_hash": parsed["anthropic"]["hash"],
                "openai_raw_response_hash": parsed["openai"]["hash"]
            }
            return parsed[winner], compete_log, winner
            
        else:
            # Single mode or fallback occurred
            winner = list(parsed.keys())[0]
            if strategy == "compete":
                 logger.warning(f"Compete mode degraded to single provider: {winner}")
            return parsed[winner], None, winner

    def _expand_capped_sentinels(
        self, struct_data: dict, processed_data: dict, winner_id: str, winner_model: str
    ) -> None:
        """Add resolved entries for mixed_sentinel cols that were capped in the prompt.

        Uses the first resolved item with matching (sheet, hypothesis) as a template.
        Mutates processed_data in place.
        """
        from utils.prompt_builder import MAX_MIXED_SENTINEL_ITEMS

        # collect all sheet::field already covered by LLM
        covered: set[str] = set()
        for bucket in ("resolved", "confirm_queue", "escalate_queue"):
            for item in processed_data.get(bucket, []):
                if item.get("finding_type") == "mixed_sentinel":
                    covered.add(f"{item['sheet']}::{item['field']}")

        # build a template per sheet from the first resolved sentinel on that sheet
        templates: dict[str, dict] = {}
        for item in processed_data.get("resolved", []):
            if item.get("finding_type") == "mixed_sentinel":
                sh = item["sheet"]
                # Only use as template if it's NOT a filter_artifact — we don't
                # want to clone a filter_artifact hypothesis across sheets where
                # the same sentinel (e.g. "нет продаж") means something real.
                if sh not in templates and item.get("hypothesis") != "filter_artifact":
                    templates[sh] = item

        # walk structural data and clone missing ones
        added = 0
        for sheet_name, sheet_data in struct_data.get("sheets", {}).items():
            for col_name, col_meta in sheet_data.get("columns", {}).items():
                if not (col_meta.get("mixed") and col_meta.get("mixed_sentinel_values")):
                    continue
                key = f"{sheet_name}::{col_name}"
                if key in covered:
                    continue
                # clone from template for this sheet, or first available template
                # Only use cross-sheet template as absolute last resort,
                # and never propagate filter_artifact hypothesis across sheets.
                sheet_tmpl = templates.get(sheet_name)
                other_tmpl = next(
                    (t for t in templates.values() if t.get("hypothesis") != "filter_artifact"),
                    None
                ) if not sheet_tmpl else None
                tmpl = sheet_tmpl or other_tmpl
                if tmpl is None:
                    continue
                clone = {
                    "finding_type": "mixed_sentinel",
                    "sheet": sheet_name,
                    "field": col_name,
                    "hypothesis": tmpl["hypothesis"],
                    "confidence": tmpl["confidence"],
                    "evidence": [
                        f"Same sentinel pattern as '{tmpl['field']}' on same sheet.",
                        f"Sentinel values: {col_meta['mixed_sentinel_values']}",
                    ],
                    "provider": winner_id,
                    "model": winner_model,
                    "auto_expanded": True,
                }
                queue, routed = self._route_finding(clone)
                processed_data[queue].append(routed)
                if queue == "resolved":
                    processed_data["confidence_sum"] += routed.get("confidence", 0.0)
                covered.add(key)
                added += 1

        if added:
            logger.info(f"Auto-expanded {added} capped mixed_sentinel items by pattern cloning.")

    async def analyze(self, structural_file: Path, strategy: str = "compete") -> dict:
        """Main entrypoint for Stage 02."""
        with open(structural_file, "r", encoding="utf-8") as f:
            struct_data = json.load(f)

        user_prompt = self.prompt_builder.build_payload(struct_data)

        processed_data, compete_log, winner_id = await self._run_strategy(user_prompt, strategy)

        # Determine winner model name for cloned items
        winner_model = (
            self.anthropic_client.model if winner_id == "anthropic"
            else self.openai_client.model
        )
        self._expand_capped_sentinels(struct_data, processed_data, winner_id, winner_model)
        
        output = {
            "source_structural_file": structural_file.name,
            "analyzed_at": datetime.now().isoformat(),
            "schema_version": "1.0",
            "provider_strategy": strategy,
            "winner_provider": winner_id,
            "resolved": processed_data["resolved"],
            "confirm_queue": processed_data["confirm_queue"],
            "escalate_queue": processed_data["escalate_queue"],
            "compete_log": compete_log
        }

             
        # Validate schema before returning
        try:
             jsonschema.validate(instance=output, schema=self.output_schema)
        except jsonschema.exceptions.ValidationError as e:
             logger.error(f"Semantic Output Validation Failed: {e}")
             
        return output

if __name__ == "__main__":
    import argparse
    import sys
    import os
    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(description="Stage 02 - Semantic Analyzer")
    parser.add_argument("--input", "-i", type=str, required=True, help="Path to structural JSON")
    parser.add_argument("--strategy", "-s", type=str, default="compete", choices=["compete", "single_anthropic", "single_openai"], help="Provider strategy")
    args = parser.parse_args()

    load_dotenv()
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
    openai_key = os.getenv("OPENAI_API_KEY", "")

    if not anthropic_key and not openai_key:
        print("ERROR: API keys missing. Set ANTHROPIC_API_KEY and OPENAI_API_KEY in .env.", file=sys.stderr)
        sys.exit(1)

    # Initialize analyzer
    analyzer = SemanticAnalyzer(anthropic_key=anthropic_key, openai_key=openai_key)

    try:
        result = asyncio.run(analyzer.analyze(Path(args.input), strategy=args.strategy))
    except Exception as e:
        print(f"Analysis failed: {e}", file=sys.stderr)
        sys.exit(1)

    out_dir = Path("outputs")
    out_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    out_path = out_dir / f"semantic_{timestamp}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"Saved: {out_path}", file=sys.stderr)
