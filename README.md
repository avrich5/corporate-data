# corporate-data

**Excel Analytics Pipeline** — автоматична побудова структурованої схеми даних з корпоративних Excel-файлів із використанням LLM.

Демонструє, як перетворити сотні Excel-таблиць, накопичених роками, на машиночитану базу знань — основу для корпоративного data agent.

---

## Задача

Організації накопичують сотні Excel-файлів побудованих фахівцями предметної галузі. Файли кодують критичну бізнес-логіку (пайплайни, ієрархії, порогові значення), яка існує лише в структурі файлів та в головах авторів.

**Мета:** автоматично витягнути цю структуру у `final_schema.json` — семантичний шар (data catalog) для майбутнього query agent.

---

## Архітектура

```
Excel
  │
  ▼
Stage 01b — LLM Navigation     ← визначає header_row, data_start, period blocks
  │
  ▼
Stage 01  — Structural Extractor  ← механічні факти (типи, null rate, sentinels)
  │
  ▼
Stage 02  — Semantic Analyzer (LLM) ← класифікує поля, виявляє зв'язки
  │
  ├── resolved[]        confidence > 0.85  → автоматично
  ├── confirm_queue[]   confidence 0.5–0.85 → human review
  └── escalate_queue[]  confidence < 0.5   → людина відповідає
  │
  ▼
Stage 03  — Human Review (CLI)
  │
  ▼
Stage 04  — Schema Assembler
  │
  ├── final_schema_<timestamp>.json   ← data catalog
  └── report_<timestamp>.docx         ← звіт для стейкхолдерів
```

Кожен stage — окремий модуль, комунікує через файли (ADR-001). Джерелний Excel ніколи не модифікується (ADR-003).

---

## Швидкий старт

### 1. Клонувати і налаштувати

```bash
git clone https://github.com/avrich5/corporate-data.git
cd corporate-data
cp .env.example .env
# заповніть ANTHROPIC_API_KEY та/або OPENAI_API_KEY
```

### 2. Додати Excel-файл

```bash
mkdir -p sources
cp /path/to/your_file.xlsx sources/
```

### 3. Запустити pipeline

```bash
./run_pipeline.sh
```

Або з явним файлом і стратегією:

```bash
./run_pipeline.sh sources/your_file.xlsx --strategy compete
./run_pipeline.sh sources/your_file.xlsx --skip-stage03
```

**Стратегії провайдера (`--strategy`):**

| Значення | Опис |
|---|---|
| `compete` | Запускає Anthropic + OpenAI паралельно, бере відповідь з вищою confidence |
| `single_anthropic` | Тільки Claude |
| `single_openai` | Тільки GPT-4o |

### 4. Результати

```
outputs/
  navigation_<timestamp>.json    Stage 01b: координати заголовків та блоків
  structural_<timestamp>.json    Stage 01: структурний скан
  semantic_<timestamp>.json      Stage 02: семантичний аналіз LLM
  human_review_<timestamp>.json  Stage 03: відповіді людини
  final_schema_<timestamp>.json  Stage 04: фінальна схема (data catalog)
  report_<timestamp>.docx        Stage 04: звіт .docx
```

---

## Встановлення залежностей вручну

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`run_pipeline.sh` робить це автоматично.

---

## Змінні середовища

Всі параметри задаються через `.env`. Дефолти — у `config.py`.

| Змінна | Обов'язкова | Опис |
|---|---|---|
| `ANTHROPIC_API_KEY` | одна з двох | Claude API key |
| `OPENAI_API_KEY` | одна з двох | OpenAI API key |
| `LLM_PROVIDER_STRATEGY` | ні | `compete` / `single_anthropic` / `single_openai` |
| `LLM_MAX_CONTEXT_TOKENS` | ні | Розмір контексту для LLM (default: 8000) |
| `CONFIDENCE_RESOLVED_THRESHOLD` | ні | Поріг авторозв'язання (default: 0.85) |
| `CONFIDENCE_CONFIRM_THRESHOLD` | ні | Поріг підтвердження (default: 0.50) |
| `LOG_LEVEL` | ні | `INFO` / `DEBUG` |
| `REPORT_LANGUAGE` | ні | `ru` / `uk` / `en` |

Повний перелік із коментарями — у `.env.example`.

---

## Тести

```bash
pytest tests/ -v
```

Fixtures з анонімізованими даними — у `tests/fixtures/`.

---

## Структура проекту

```
corporate-data/
├── pipeline/
│   ├── stage_01b_navigate.py    LLM: визначає координати аркушів
│   ├── stage_01_extract.py      структурний скан (без LLM)
│   ├── stage_02_analyze.py      LLM семантичний аналіз
│   ├── stage_03_review.py       CLI human review
│   └── stage_04_assemble.py     збирає final_schema + .docx
├── utils/
│   ├── excel_reader.py          Excel I/O, детекція заголовків
│   ├── llm_client.py            Anthropic/OpenAI wrapper, retry, логування
│   ├── prompt_builder.py        будує Jinja2 промпти з структурного JSON
│   ├── report_writer.py         генерація .docx звіту
│   ├── review_grouper.py        групування confirm/escalate черги
│   ├── review_session.py        стан CLI-сесії рев'ю
│   └── schema_validator.py      JSON Schema валідація між stages
├── prompts/
│   └── semantic_analyzer.jinja2 шаблон промпту Stage 02
├── tests/
│   ├── fixtures/                анонімізовані Excel-файли для тестів
│   ├── test_stage_01.py
│   ├── test_stage_02.py
│   └── test_stage_04.py
├── config.py                    єдине джерело всіх параметрів
├── run_pipeline.sh              повний запуск одною командою
├── requirements.txt             залежності з пінованими версіями
├── .env.example                 шаблон змінних середовища
└── ARCHITECTURE.md              ADR-лог, детальна архітектура
```

---

## Дорожня карта

- [x] Stage 01b — LLM Navigation (визначення структури аркушів)
- [x] Stage 01 — Structural Extractor
- [x] Stage 02 — Semantic Analyzer (compete mode: Anthropic vs OpenAI)
- [x] Stage 03 — Human Review CLI
- [x] Stage 04 — Schema Assembler + .docx report
- [ ] Stage 05 — Query Agent (природньомовні запити → Python/pandas → результат)
- [ ] Stage 05a — Vector Store (ChromaDB ембеддінги схеми)
- [ ] Stage 05b — Retrieval Layer
- [ ] Stage 05c — Text-to-Python Generator
- [ ] Stage 05d — Sandbox Executor
- [ ] Stage 05e — Chart Builder
- [ ] Streamlit UI для оператора

---

## Ліцензія

MIT
