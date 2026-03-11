import json
import logging
from pathlib import Path
from typing import Optional

import config
from jinja2 import Environment, FileSystemLoader
from utils.llm_client import AnthropicClient, OpenAIClient

logger = logging.getLogger(__name__)


class QueryGenerator:
    """Generates Python code from natural language using the final data schema."""
    
    def __init__(self, schema_path: Path):
        self.schema_path = schema_path
        with open(schema_path, "r", encoding="utf-8") as f:
            self.schema_data = json.load(f)
            
        env = Environment(loader=FileSystemLoader(str(config.PROMPTS_DIR)))
        self.template = env.get_template("query_generator.jinja2")
        
        provider_model = config.QUERY_GEN_MODEL
        if "claude" in provider_model.lower() or "anthropic" in provider_model.lower():
            self.llm = AnthropicClient(api_key=config.ANTHROPIC_API_KEY, model=provider_model)
        else:
            self.llm = OpenAIClient(api_key=config.OPENAI_API_KEY, model=provider_model)

    @staticmethod
    def _clean_code_output(raw: str) -> str:
        """Strips markdown fences from python code LLM output."""
        s = raw.strip()
        if "```python" in s:
            s = s.split("```python", 1)[1].split("```")[0].strip()
        elif "```" in s:
            s = s.split("```", 1)[1].split("```")[0].strip()
        return s

    async def generate_code(
        self,
        query: str,
        error_context: Optional[str] = None,
        previous_code: Optional[str] = None
    ) -> str:
        """Generates Python code for a user query.
        
        Args:
            query: The natural language user query.
            error_context: If retrying, the error message from previous attempt.
            previous_code: If retrying, the previously failed code.
            
        Returns:
            The generated python code snippet as string.
        """
        # P1 strategy: pass entire schema JSON in the prompt as string context
        # In P3 this will be swapped for retrieval logic top-k output
        schema_str = json.dumps(self.schema_data, ensure_ascii=False, indent=2)
        
        # Approximate 1 token ~ 4 chars for rough truncation safety
        max_chars = config.QUERY_CONTEXT_MAX_TOKENS * 4
        if len(schema_str) > max_chars:
            logger.warning(f"Schema length ({len(schema_str)}) exceeds context cap. Truncating.")
            schema_str = schema_str[:max_chars] + "\n... (truncated)"
            
        prompt_payload = self.template.render(
            query=query,
            schema=schema_str,
            error_context=error_context,
            previous_code=previous_code
        )
        
        system_prompt = (
            "You are an expert Data Analyst and Python developer. "
            "You write safe, exact pandas code to answer user queries using the provided schema."
        )
        
        logger.info(f"Generating query code... (model={self.llm.model})")
        resp = await self.llm.complete(
            prompt=prompt_payload,
            system=system_prompt,
            max_tokens=config.QUERY_GEN_MAX_TOKENS,
            temperature=config.QUERY_GEN_TEMPERATURE,
            json_mode=False,  # code generation — never JSON mode
        )

        return self._clean_code_output(resp.content)


if __name__ == "__main__":
    import argparse
    import asyncio
    from dotenv import load_dotenv
    
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Stage 05c - Query Generator")
    parser.add_argument("--schema", type=Path, required=True, help="Path to final_schema.json")
    parser.add_argument("--query", type=str, required=True, help="Natural language query")
    args = parser.parse_args()
    
    logging.basicConfig(level=getattr(logging, config.LOG_LEVEL), format="%(levelname)s: %(message)s")
    
    async def main() -> None:
        generator = QueryGenerator(schema_path=args.schema)
        code = await generator.generate_code(query=args.query)
        print("\n--- GENERATED CODE ---\n")
        print(code)
        
    asyncio.run(main())
