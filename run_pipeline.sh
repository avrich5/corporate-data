#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh — полный запуск пайплайна от Excel до .docx
# =============================================================================
# Usage:
#   ./run_pipeline.sh
#   ./run_pipeline.sh sources/file.xlsx
#   ./run_pipeline.sh --skip-stage03
#   ./run_pipeline.sh sources/file.xlsx --skip-stage03
#   ./run_pipeline.sh --strategy single_anthropic
# =============================================================================
set -euo pipefail

GREEN='\033[0;32m'; BLUE='\033[0;34m'
YELLOW='\033[1;33m'; RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'

log()  { echo -e "${BLUE}[pipeline]${NC} $*"; }
ok()   { echo -e "${GREEN}[✓]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
fail() { echo -e "${RED}[✗] $*${NC}"; exit 1; }
sep()  { echo -e "${BLUE}────────────────────────────────────────────────────${NC}"; }

EXCEL_FILE=""; SKIP_S03=0; STRATEGY="compete"

for arg in "$@"; do
  case "$arg" in
    --skip-stage03)   SKIP_S03=1 ;;
    --strategy=*)     STRATEGY="${arg#--strategy=}" ;;
    compete|single_anthropic|single_openai) STRATEGY="$arg" ;;
    *.xlsx|*.xls)     EXCEL_FILE="$arg" ;;
    *) warn "Unknown argument ignored: $arg" ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
mkdir -p outputs logs

echo ""
echo -e "${BOLD}${BLUE}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${BLUE}║      corporate-data pipeline  ·  full run           ║${NC}"
echo -e "${BOLD}${BLUE}╚══════════════════════════════════════════════════════╝${NC}"
echo ""

# =============================================================================
# VENV
# =============================================================================
VENV_DIR=".venv"

if [[ ! -f "$VENV_DIR/bin/activate" ]]; then
  log "Creating virtual environment: $VENV_DIR"
  python3 -m venv "$VENV_DIR"
  ok "Created $VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
log "Using Python: $(which python3)  ($(python3 --version))"

if [[ ! -f "$VENV_DIR/.deps_installed" ]] \
   || [[ requirements.txt -nt "$VENV_DIR/.deps_installed" ]]; then
  log "Installing dependencies from requirements.txt..."
  pip install --quiet --upgrade pip
  pip install --quiet -r requirements.txt
  touch "$VENV_DIR/.deps_installed"
  ok "Dependencies installed"
else
  ok "Dependencies up-to-date"
fi

# =============================================================================
# ENV
# =============================================================================
[[ -f ".env" ]] || fail ".env not found — copy .env.example and fill in API keys"
set -a; source .env; set +a

[[ -n "${ANTHROPIC_API_KEY:-}" || -n "${OPENAI_API_KEY:-}" ]] \
  || fail "No API keys in .env (need ANTHROPIC_API_KEY or OPENAI_API_KEY)"

ok "Environment OK  (strategy=${STRATEGY})"
echo ""

# =============================================================================
# STAGE 01 — Structural Extractor  (with LLM Navigation Pre-pass)
# =============================================================================

if [[ -z "$EXCEL_FILE" ]]; then
  EXCEL_FILE=$(ls -t sources/*.xlsx sources/*.xls 2>/dev/null | head -1 || true)
  [[ -n "$EXCEL_FILE" ]] || fail "No .xlsx found in sources/ — pass path as argument"
  log "Auto-detected: $EXCEL_FILE"
fi
[[ -f "$EXCEL_FILE" ]] || fail "File not found: $EXCEL_FILE"

# ---------------------------------------------------------------------------
# Stage 01b — LLM Navigation Pass
# Sends first 15 + last 3 rows of each sheet to the LLM.
# LLM returns: header_row, data_start_row, layout type, period block coords.
# Stage 01 (structural extractor) reads the latest navigation_*.json
# automatically and uses its coordinates instead of the heuristic detector.
# ---------------------------------------------------------------------------
sep
echo -e "${BOLD} Stage 01b — LLM Navigation Pass${NC}"
sep

python3 -m pipeline.stage_01b_navigate --input "$EXCEL_FILE"

NAVIGATION=$(ls -t outputs/navigation_*.json 2>/dev/null | head -1 || true)
if [[ -n "$NAVIGATION" ]]; then
  ok "navigation → $NAVIGATION"
  python3 -c "
import json
d = json.load(open('$NAVIGATION'))
sheets = d.get('sheets', {})
low      = [n for n,v in sheets.items() if v.get('confidence',1) < 0.7]
hperiod  = [n for n,v in sheets.items() if v.get('layout') == 'horizontal_periods']
print(f'         model={d[\"model\"]}  sheets={len(sheets)}  horizontal_periods={len(hperiod)}  low_conf={len(low)}')
if low:
    print(f'         ⚠ low confidence: {low}')
"
else
  warn "Stage 01b produced no navigation file — Stage 01 will use heuristics"
fi
echo ""

# ---------------------------------------------------------------------------
# Stage 01 — Full structural extraction
# ---------------------------------------------------------------------------
sep
echo -e "${BOLD} Stage 01 — Structural Extractor${NC}"
sep

python3 -m pipeline.stage_01_extract --input "$EXCEL_FILE"

STRUCTURAL=$(ls -t outputs/structural_*.json 2>/dev/null | head -1 \
  || fail "Stage 01 produced no output")
ok "structural → $STRUCTURAL"

python3 -c "
import json
d = json.load(open('$STRUCTURAL'))
raw = d.get('sheets', d.get('tables', {}))
sheets = list(raw.values()) if isinstance(raw, dict) else raw
total_cols = sum(len(s.get('columns', [])) for s in sheets if isinstance(s, dict))
print(f'         sheets={len(sheets)}  total_columns={total_cols}')
"
echo ""

# =============================================================================
# STAGE 02 — Semantic Analyzer (LLM)
# =============================================================================
sep
echo -e "${BOLD} Stage 02 — Semantic Analyzer (LLM: ${STRATEGY})${NC}"
sep

python3 -m pipeline.stage_02_analyze \
  --input    "$STRUCTURAL" \
  --strategy "$STRATEGY"

SEMANTIC=$(ls -t outputs/semantic_*.json 2>/dev/null | head -1 \
  || fail "Stage 02 produced no output — check API keys")
ok "semantic → $SEMANTIC"

python3 -c "
import json
d = json.load(open('$SEMANTIC'))
resolved = len(d.get('resolved', []))
confirm  = len(d.get('confirm_queue', []))
escalate = len(d.get('escalate_queue', []))
winner   = d.get('winner_provider', '—')
print(f'         winner={winner}  resolved={resolved}  confirm_queue={confirm}  escalate_queue={escalate}')
"
echo ""

# =============================================================================
# STAGE 03 — Human Review
# =============================================================================
sep
echo -e "${BOLD} Stage 03 — Human Review${NC}"
sep

REVIEW_FILE=""

if [[ $SKIP_S03 -eq 1 ]]; then
  warn "Skipped (--skip-stage03). Only auto-resolved items go to final schema."
else
  QUEUE_SIZE=$(python3 -c "
import json
d = json.load(open('$SEMANTIC'))
print(len(d.get('confirm_queue', [])) + len(d.get('escalate_queue', [])))
")
  if [[ "$QUEUE_SIZE" -eq 0 ]]; then
    ok "Nothing to review — all items auto-resolved"
  else
    log "$QUEUE_SIZE items need human review. Starting interactive session..."
    echo ""
    python3 -m pipeline.stage_03_review --input "$SEMANTIC"
    REVIEW_FILE=$(ls -t outputs/human_review_*.json 2>/dev/null | head -1 || true)
    [[ -n "$REVIEW_FILE" ]] && ok "human_review → $REVIEW_FILE" \
      || warn "Stage 03 finished but no review file saved"
  fi
fi
echo ""

# =============================================================================
# STAGE 04 — Schema Assembler + Report Writer
# =============================================================================
sep
echo -e "${BOLD} Stage 04 — Schema Assembler + Report Writer${NC}"
sep

S04_ARGS="--structural $STRUCTURAL --semantic $SEMANTIC"
[[ -n "$REVIEW_FILE" ]] && S04_ARGS="$S04_ARGS --human $REVIEW_FILE"

python3 -m pipeline.stage_04_assemble $S04_ARGS

SCHEMA=$(ls -t outputs/final_schema_*.json 2>/dev/null | head -1 \
  || fail "Stage 04 produced no schema JSON")
REPORT=$(ls -t outputs/report_*.docx 2>/dev/null | head -1 \
  || fail "Stage 04 produced no .docx report")

ok "final_schema → $SCHEMA"
ok "report       → $REPORT"

python3 -c "
try:
    from docx import Document
    doc = Document('$REPORT')
    h1 = sum(1 for p in doc.paragraphs if p.style.name == 'Heading 1')
    print(f'         paragraphs={len(doc.paragraphs)}  tables={len(doc.tables)}  H1={h1}')
except Exception as e:
    print(f'         (stats unavailable: {e})')
" 2>/dev/null || true
echo ""

# =============================================================================
# ИТОГ
# =============================================================================
echo -e "${BOLD}${GREEN}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${GREEN}║                Pipeline complete ✓                  ║${NC}"
echo -e "${BOLD}${GREEN}╚══════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${BOLD}Input Excel:${NC}    $EXCEL_FILE"
[[ -n "$NAVIGATION" ]] && echo -e "  ${BOLD}Navigation:${NC}     $NAVIGATION"
echo -e "  ${BOLD}Structural:${NC}     $STRUCTURAL"
echo -e "  ${BOLD}Semantic:${NC}       $SEMANTIC"
[[ -n "$REVIEW_FILE" ]] && echo -e "  ${BOLD}Human review:${NC}  $REVIEW_FILE"
echo -e "  ${BOLD}Final schema:${NC}   $SCHEMA"
echo -e "  ${BOLD}Report:${NC}         ${GREEN}$REPORT${NC}"
echo ""
echo -e "  Открыть: ${BOLD}open \"$REPORT\"${NC}"
echo ""

[[ "$OSTYPE" == "darwin"* ]] && open "$REPORT" || true
