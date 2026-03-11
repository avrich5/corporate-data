"""
config.py — single source of truth for all pipeline parameters.
All values overridable via environment variables.
No magic numbers anywhere else in the codebase.
"""
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()  # loads .env before any os.getenv() calls
except ImportError:
    pass  # python-dotenv not installed — rely on env vars set externally

# ─── Paths ────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
OUTPUTS_DIR = BASE_DIR / "outputs"
LOGS_DIR = BASE_DIR / "logs"
PROMPTS_DIR = BASE_DIR / "prompts"
SPECS_DIR = BASE_DIR / "specs"

# ─── LLM Providers ────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")

# Strategy: "single_anthropic" | "single_openai" | "compete"
# compete: run both providers, pick result with higher confidence sum
LLM_PROVIDER_STRATEGY = os.getenv("LLM_PROVIDER_STRATEGY", "compete")

# ─── LLM Parameters ───────────────────────────────────────────
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "4096"))
LLM_MAX_CONTEXT_TOKENS = int(os.getenv("LLM_MAX_CONTEXT_TOKENS", "8000"))
LLM_TEMPERATURE_CLASSIFY = float(os.getenv("LLM_TEMPERATURE_CLASSIFY", "0.0"))
LLM_TEMPERATURE_GENERATE = float(os.getenv("LLM_TEMPERATURE_GENERATE", "0.3"))
LLM_RETRY_MAX = int(os.getenv("LLM_RETRY_MAX", "3"))
LLM_RETRY_BASE_DELAY = float(os.getenv("LLM_RETRY_BASE_DELAY", "2.0"))

# ─── Confidence Routing ───────────────────────────────────────
CONFIDENCE_RESOLVED_THRESHOLD = float(
    os.getenv("CONFIDENCE_RESOLVED_THRESHOLD", "0.85")
)
CONFIDENCE_CONFIRM_THRESHOLD = float(
    os.getenv("CONFIDENCE_CONFIRM_THRESHOLD", "0.50")
)
# Below CONFIRM_THRESHOLD -> escalate_queue (human must answer)

# ─── Compete Mode ─────────────────────────────────────────────
# When strategy == "compete":
# Both providers run in parallel (asyncio.gather).
# Winner = provider whose resolved[] items have higher avg confidence.
# If one provider fails — fallback to the other silently.
# Both raw responses logged to audit.log for review.
COMPETE_LOG_BOTH = bool(os.getenv("COMPETE_LOG_BOTH", "true"))

# ─── Structural Extraction ────────────────────────────────────
HEADER_SEARCH_MAX_ROWS = int(os.getenv("HEADER_SEARCH_MAX_ROWS", "10"))
SAMPLE_ROWS_FOR_LLM = int(os.getenv("SAMPLE_ROWS_FOR_LLM", "5"))
MIXED_TYPE_MIN_RATIO = float(os.getenv("MIXED_TYPE_MIN_RATIO", "0.05"))
NULL_RATE_HIGH_THRESHOLD = float(os.getenv("NULL_RATE_HIGH_THRESHOLD", "0.5"))
UNIQUE_VALUES_CATEGORICAL_MAX = int(
    os.getenv("UNIQUE_VALUES_CATEGORICAL_MAX", "50")
)

# ─── Logging ──────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "json"

# ─── Output Naming ────────────────────────────────────────────
TIMESTAMP_FORMAT = "%Y-%m-%dT%H-%M-%S"

# ─── Report ───────────────────────────────────────────────────
REPORT_LANGUAGE = os.getenv("REPORT_LANGUAGE", "ru")

# ─── Stage 05: Vector Store ───────────────────────────────────
EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "openai")
# "openai" → text-embedding-3-small (requires OPENAI_API_KEY)
# "local"  → sentence-transformers/all-MiniLM-L6-v2 (no API, slower)
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
VECTOR_STORE_DIR = Path(os.getenv("VECTOR_STORE_DIR", str(OUTPUTS_DIR / "vector_store")))
RETRIEVAL_TOP_K = int(os.getenv("RETRIEVAL_TOP_K", "10"))
RETRIEVAL_MIN_SCORE = float(os.getenv("RETRIEVAL_MIN_SCORE", "0.55"))

# ─── Stage 05: Query Generator ────────────────────────────────
QUERY_GEN_MODEL = os.getenv("QUERY_GEN_MODEL", ANTHROPIC_MODEL)
QUERY_GEN_MAX_TOKENS = int(os.getenv("QUERY_GEN_MAX_TOKENS", "2048"))
QUERY_GEN_TEMPERATURE = float(os.getenv("QUERY_GEN_TEMPERATURE", "0.0"))
QUERY_CONTEXT_MAX_TOKENS = int(os.getenv("QUERY_CONTEXT_MAX_TOKENS", "6000"))
QUERY_SAMPLE_ROWS = int(os.getenv("QUERY_SAMPLE_ROWS", "3"))
QUERY_MAX_RETRIES = int(os.getenv("QUERY_MAX_RETRIES", "2"))

# ─── Stage 05: Executor ───────────────────────────────────────
EXECUTOR_TIMEOUT = int(os.getenv("EXECUTOR_TIMEOUT", "30"))
EXECUTOR_MAX_ROWS = int(os.getenv("EXECUTOR_MAX_ROWS", "10000"))
EXECUTOR_SAVE_CODE = os.getenv("EXECUTOR_SAVE_CODE", "true").lower() == "true"
EXECUTOR_ALLOWED_IMPORTS: list[str] = [
    "pandas", "numpy", "datetime", "re", "json", "collections", "math",
]

# ─── Stage 05: Chart Builder ──────────────────────────────────
CHART_DEFAULT_TYPE = os.getenv("CHART_DEFAULT_TYPE", "auto")
# "auto" | "bar" | "barh" | "line" | "scatter" | "pie"
CHART_OUTPUT_FORMAT = os.getenv("CHART_OUTPUT_FORMAT", "png")
# "png" | "html" | "both"
CHART_DPI = int(os.getenv("CHART_DPI", "150"))
CHART_LOCALE = os.getenv("CHART_LOCALE", "ru_RU")
CHART_COLORMAP = os.getenv("CHART_COLORMAP", "tab10")

# (duplicate Stage 05 block removed — defined above)
