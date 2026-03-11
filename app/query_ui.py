import streamlit as st
import pandas as pd
import json
import logging
from pathlib import Path
import time
import asyncio

import config
from pipeline.stage_05c_generate import QueryGenerator
from pipeline.stage_05d_execute import execute_query
from pipeline.stage_05e_chart import ChartBuilder

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Corporate Data - Query Agent", layout="wide")


def get_latest_file(directory: Path, pattern: str) -> Path:
    files = list(directory.glob(pattern))
    if not files:
        return None
    return sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)[0]


def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


def load_data(excel_path: Path):
    if "dfs" not in st.session_state:
        st.session_state.dfs = pd.read_excel(excel_path, sheet_name=None)


def init_session_state():
    if "config" not in st.session_state:
        st.session_state.config = {
            "ANTHROPIC_API_KEY": config.ANTHROPIC_API_KEY,
            "OPENAI_API_KEY": config.OPENAI_API_KEY,
            "QUERY_GEN_MODEL": config.QUERY_GEN_MODEL,
            "QUERY_CONTEXT_MAX_TOKENS": config.QUERY_CONTEXT_MAX_TOKENS,
            "QUERY_SAMPLE_ROWS": config.QUERY_SAMPLE_ROWS,
            "QUERY_GEN_MAX_TOKENS": config.QUERY_GEN_MAX_TOKENS,
            "QUERY_GEN_TEMPERATURE": config.QUERY_GEN_TEMPERATURE,
            "QUERY_MAX_RETRIES": config.QUERY_MAX_RETRIES,
            "EXECUTOR_TIMEOUT": config.EXECUTOR_TIMEOUT,
            "EXECUTOR_MAX_ROWS": config.EXECUTOR_MAX_ROWS,
            "CHART_DEFAULT_TYPE": config.CHART_DEFAULT_TYPE,
            "CHART_OUTPUT_FORMAT": config.CHART_OUTPUT_FORMAT,
            "CHART_DPI": config.CHART_DPI,
            "CHART_COLORMAP": config.CHART_COLORMAP,
        }

init_session_state()

# ─── SIDEBAR (Config) ───────────────────────────────────────────────────
st.sidebar.title("Налаштування")

st.sidebar.subheader("API Ключі")
st.session_state.config["ANTHROPIC_API_KEY"] = st.sidebar.text_input("Anthropic API Key", type="password", value=st.session_state.config["ANTHROPIC_API_KEY"])
st.session_state.config["OPENAI_API_KEY"] = st.sidebar.text_input("OpenAI API Key", type="password", value=st.session_state.config["OPENAI_API_KEY"])
st.session_state.config["QUERY_GEN_MODEL"] = st.sidebar.selectbox("Генератор (Model)", ["claude-sonnet-4-6", "claude-3-5-sonnet-20241022", "gpt-4o", "gpt-4-turbo"], index=0)

st.sidebar.subheader("Генератор")
st.session_state.config["QUERY_CONTEXT_MAX_TOKENS"] = st.sidebar.slider("Контекст (tokens)", 1000, 12000, st.session_state.config["QUERY_CONTEXT_MAX_TOKENS"], 500)
st.session_state.config["QUERY_SAMPLE_ROWS"] = st.sidebar.slider("Рядків прикладу", 1, 10, st.session_state.config["QUERY_SAMPLE_ROWS"], 1)
st.session_state.config["QUERY_GEN_MAX_TOKENS"] = st.sidebar.slider("Макс. довжина (tokens)", 512, 4096, st.session_state.config["QUERY_GEN_MAX_TOKENS"], 128)
st.session_state.config["QUERY_GEN_TEMPERATURE"] = st.sidebar.slider("Температура", 0.0, 1.0, float(st.session_state.config["QUERY_GEN_TEMPERATURE"]), 0.1)
st.session_state.config["QUERY_MAX_RETRIES"] = st.sidebar.slider("Кількість retry", 0, 3, st.session_state.config["QUERY_MAX_RETRIES"], 1)

st.sidebar.subheader("Executor (Безпека)")
st.session_state.config["EXECUTOR_TIMEOUT"] = st.sidebar.slider("Таймаут (сек)", 5, 120, st.session_state.config["EXECUTOR_TIMEOUT"], 5)
st.session_state.config["EXECUTOR_MAX_ROWS"] = st.sidebar.number_input("Ліміт рядків", min_value=100, max_value=100000, value=st.session_state.config["EXECUTOR_MAX_ROWS"], step=1000)

st.sidebar.subheader("Графіки")
st.session_state.config["CHART_DEFAULT_TYPE"] = st.sidebar.selectbox("Автовибір графіку", ["auto", "bar", "line", "scatter", "pie"], index=0)
st.session_state.config["CHART_OUTPUT_FORMAT"] = st.sidebar.radio("Формат виводу", ["png", "html", "both"], index=0)
st.session_state.config["CHART_DPI"] = st.sidebar.selectbox("Роздільність DPI", [72, 100, 150, 300], index=2)
st.session_state.config["CHART_COLORMAP"] = st.sidebar.selectbox("Колірна палітра", ["tab10", "viridis", "Set1", "Pastel1"], index=0)

if st.sidebar.button("Зберегти конфігурацію сесії"):
    # session_state is already updated via widget bindings above.
    # We do NOT write to disk or mutate config module — session only.
    st.sidebar.success("Збережено в session_state (до рестарту)")


st.sidebar.subheader("Джерела даних")
sources_dir = config.BASE_DIR / "sources"
outputs_dir = config.BASE_DIR / "outputs"

excel_files = list(sources_dir.glob("*.xlsx"))
schema_files = list(outputs_dir.glob("final_schema_*.json"))

excel_path = st.sidebar.selectbox("Excel файл", excel_files, format_func=lambda x: x.name if x else "Не знайдено")
schema_path = st.sidebar.selectbox("Схема даних (Schema)", schema_files, format_func=lambda x: x.name if x else "Не знайдено")

if st.sidebar.button("Перебудувати vector store", type="primary"):
    if schema_path:
        with st.spinner("Будуємо vector store..."):
            from pipeline.stage_05a_embed import SchemaEmbedder
            import json
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_data = json.load(f)
            embedder = SchemaEmbedder()
            embedder.embed_schema(schema_data)
        st.sidebar.success("Vector store оновлено!")
    else:
        st.sidebar.error("Виберіть final_schema.json")

if excel_path:
    with st.spinner("Завантаження даних..."):
        load_data(excel_path)
    st.sidebar.success(f"Завантажено аркушів: {len(st.session_state.dfs)}")


# ─── MAIN PANEL ──────────────────────────────────────────────────────────
st.title("Аналіз даних (Query Agent)")

query = st.text_area("Введіть ваш запит природньою мовою (наприклад: 'Топ-5 категорій по доходу за IV кв 2025')", height=100)

col1, col2 = st.columns([1, 8])
with col1:
    execute_clicked = st.button("Виконати", type="primary", use_container_width=True)

st.markdown("---")

if execute_clicked and query:
    if not st.session_state.config["ANTHROPIC_API_KEY"] and not st.session_state.config["OPENAI_API_KEY"]:
        st.error("API ключ не заданий. Додайте ключ у сайдбарі.")
        st.stop()
        
    if not schema_path:
        st.error("Не знайдено final_schema.json.")
        st.stop()
        
    if "dfs" not in st.session_state:
        st.error("Дані Excel не завантажено.")
        st.stop()

    progress_bar = st.progress(0, text="Очікування...")
    
    # 1. Retrieval
    progress_bar.progress(25, text="Retrieval... (пошук релевантних полів)")
    from pipeline.stage_05b_retrieve import SchemaRetriever
    try:
        retriever = SchemaRetriever()
        retrieved_schema = retriever.retrieve(
            query=query,
            top_k=st.session_state.config.get("RETRIEVAL_TOP_K", 10),
            min_score=st.session_state.config.get("RETRIEVAL_MIN_SCORE", 0.55)
        )
        
        # Count total retrieved columns for UI stat
        total_cols = sum(len(t.get("columns", [])) for t in retrieved_schema.get("tables", []))
        progress_bar.progress(35, text=f"Retrieval... знайдено {total_cols} полів")
        time.sleep(0.5)
        
    except Exception as retrieval_err:
        progress_bar.empty()
        logger.error("Retrieval failed: %s", retrieval_err, exc_info=True)
        st.warning("⚠️ Не вдалося отримати відповідь на ваш запит.")
        st.info("Спробуйте переформулювати запит або зверніться до адміністратора.")
        if st.button("Переформулювати", key="rephrase_retrieval"):
            st.rerun()
        st.stop()

    # 2. Generation 
    progress_bar.progress(50, text="Generation... (Генерація Python-коду)")
    try:
        generator = QueryGenerator(schema_path=schema_path)
        # Pass retrieved sub-schema instead of full schema
        # Generator serializes self.schema_data in generate_code — replacing it here
        # is correct: no disk write, no config mutation
        generator.schema_data = retrieved_schema
        
        # Apply session_state config overrides to generator (no mutation of module-level config)
        selected_model = st.session_state.config["QUERY_GEN_MODEL"]
        if "claude" in selected_model.lower():
            from utils.llm_client import AnthropicClient
            generator.llm = AnthropicClient(
                api_key=st.session_state.config["ANTHROPIC_API_KEY"],
                model=selected_model,
            )
        else:
            from utils.llm_client import OpenAIClient
            generator.llm = OpenAIClient(
                api_key=st.session_state.config["OPENAI_API_KEY"],
                model=selected_model,
            )
        
        err_msg = None
        prev_code = None
        result_df = None
        
        for attempt in range(st.session_state.config["QUERY_MAX_RETRIES"] + 1):
            if attempt > 0:
                progress_bar.progress(50 + attempt*5, text=f"Generation... (Retry {attempt})")
                
            code_str = run_async(
                generator.generate_code(query=query, error_context=err_msg, previous_code=prev_code)
            )
            
            # 3. Execution
            progress_bar.progress(75, text=f"Execution... (Виконання коду, спроба {attempt+1})")
            
            payload, err = execute_query(code_str, st.session_state.dfs)
            
            if err:
                err_msg = err
                prev_code = code_str
                # Keep trying loop
            else:
                result_df = payload
                err_msg = None
                break
                
        if err_msg:
            progress_bar.empty()
            # Audit log — operator never sees this
            logger.warning("failed_auto | query=%r | last_error=%s", query, err_msg)
            st.warning("⚠️ Не вдалося отримати відповідь на ваш запит.")
            st.info("Спробуйте:\n• Уточнити запит (вказати квартал, категорію)\n• Зменшити складність фільтрів\n• Звернутися до адміністратора")
            if st.button("Переформулювати", key="rephrase_main"):
                st.rerun()
            st.stop()
            
    except Exception as base_e:
        progress_bar.empty()
        logger.error(f"Query pipeline error: {base_e}", exc_info=True)
        st.warning("⚠️ Не вдалося отримати відповідь на ваш запит.")
        st.info("Спробуйте переформулювати запит або зверніться до адміністратора.")
        if st.button("Переформулювати", key="rephrase_exc"):
            st.rerun()
        st.stop()

    # 4. Charting
    progress_bar.progress(90, text="Chart Building... (Побудова графіків)")
    
    if isinstance(result_df, pd.DataFrame):
        st.subheader("Таблиця результатів")
        st.dataframe(result_df, use_container_width=True)
        
        # Build chart
        builder = ChartBuilder(df=result_df, chart_type=st.session_state.config["CHART_DEFAULT_TYPE"])
        png_bytes, html_fig = builder.build()
        
        if png_bytes and st.session_state.config["CHART_OUTPUT_FORMAT"] in ("png", "both"):
            st.subheader("Статичний графік (PNG)")
            st.image(png_bytes)
            
        if html_fig and st.session_state.config["CHART_OUTPUT_FORMAT"] in ("html", "both"):
            st.subheader("Інтерактивний графік (HTML)")
            st.plotly_chart(html_fig, use_container_width=True)
            
    elif isinstance(result_df, pd.Series):
        st.subheader("Результат")
        st.dataframe(result_df, use_container_width=True)
    else:
        st.subheader("Результат")
        st.markdown(f"**{result_df}**")

    progress_bar.progress(100, text="Готово!")
    time.sleep(1)
    progress_bar.empty()
