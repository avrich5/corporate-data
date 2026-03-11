"""Stage 05a — Schema Embedder.

Reads final_schema.json and builds a FAISS vector index of all table/field
descriptions. The index is saved to config.VECTOR_STORE_DIR as two files:
  schema_index.faiss  — FAISS flat-L2 index
  schema_docs.json    — parallel list of {id, text, metadata} for each vector

Supports two embedding providers (config.EMBEDDING_PROVIDER):
  "openai" — text-embedding-3-small via OpenAI API  (default)
  "local"  — sentence-transformers all-MiniLM-L6-v2 (no API needed)

Usage:
  python -m pipeline.stage_05a_embed --schema outputs/final_schema_<ts>.json
"""

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import faiss
import numpy as np

import config

logger = logging.getLogger(__name__)

_INDEX_FILE = "schema_index.faiss"
_DOCS_FILE = "schema_docs.json"


def _get_embedding_fn():
    """Returns a callable: list[str] -> np.ndarray shape (N, dim)."""
    if config.EMBEDDING_PROVIDER.lower() == "local":
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer(config.EMBEDDING_MODEL
                                    if config.EMBEDDING_MODEL != "text-embedding-3-small"
                                    else "all-MiniLM-L6-v2")

        def _local_embed(texts: List[str]) -> np.ndarray:
            vecs = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
            return np.array(vecs, dtype=np.float32)

        return _local_embed
    else:
        from openai import OpenAI
        if not config.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY must be set when EMBEDDING_PROVIDER='openai'")
        client = OpenAI(api_key=config.OPENAI_API_KEY)
        model_name = config.EMBEDDING_MODEL  # text-embedding-3-small

        def _openai_embed(texts: List[str]) -> np.ndarray:
            resp = client.embeddings.create(input=texts, model=model_name)
            vecs = [item.embedding for item in resp.data]
            arr = np.array(vecs, dtype=np.float32)
            # Normalise for cosine similarity via inner product
            norms = np.linalg.norm(arr, axis=1, keepdims=True)
            arr = arr / np.maximum(norms, 1e-9)
            return arr

        return _openai_embed


def _build_documents(schema_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extracts embeddable documents from final_schema JSON."""
    tables = schema_data.get("tables", [])
    relationships = schema_data.get("relationships", [])
    docs: List[Dict[str, Any]] = []

    for table in tables:
        table_name = table.get("name", "")
        if not table_name:
            continue

        # Table-level document
        desc = table.get("description", "")
        docs.append({
            "id": f"table_{table_name}",
            "text": f"Таблиця: {table_name}. Опис: {desc}",
            "metadata": {"type": "table", "table_name": table_name, "field_name": ""},
        })

        filter_artifacts = set(table.get("filter_artifacts", []))
        for col in table.get("columns", []):
            # final_schema uses "field_name"/"data_type"; fallback to "name"/"type"
            field_name = col.get("field_name") or col.get("name", "")
            if not field_name or field_name in filter_artifacts:
                continue

            field_type = col.get("data_type") or col.get("type", "unknown")
            text = f"Таблиця {table_name}. Поле: {field_name}. Тип: {field_type}. "

            sentinels = col.get("sentinel_values") or col.get("categorical_values")
            if sentinels:
                sv = sentinels if isinstance(sentinels, str) else ", ".join(map(str, sentinels))
                text += f"Sentinel: {sv}. "

            # JOIN relationships
            related = []
            for rel in relationships:
                if rel.get("from_table") == table_name and field_name in rel.get("keys", []):
                    related.append(f"{rel['to_table']} через {', '.join(rel['keys'])}")
                elif rel.get("to_table") == table_name and field_name in rel.get("keys", []):
                    related.append(f"{rel['from_table']} через {', '.join(rel['keys'])}")
            if related:
                text += f"Пов'язана таблиця: {', '.join(related)}."

            docs.append({
                "id": f"field_{table_name}_{field_name}",
                "text": text,
                "metadata": {
                    "type": "field",
                    "table_name": table_name,
                    "field_name": field_name,
                    "sentinels": str(sentinels or ""),
                },
            })

    return docs


def build_index(schema_data: Dict[str, Any], store_dir: Path) -> Tuple[int, int]:
    """Embeds schema and writes FAISS index + docs JSON.

    Returns:
        Tuple of (num_tables, num_fields) embedded.
    """
    store_dir.mkdir(parents=True, exist_ok=True)
    docs = _build_documents(schema_data)
    if not docs:
        logger.warning("No documents found to embed — schema may be empty.")
        return 0, 0

    texts = [d["text"] for d in docs]
    logger.info("Embedding %d documents via provider=%s ...", len(texts), config.EMBEDDING_PROVIDER)
    embed_fn = _get_embedding_fn()

    # Embed in batches of 100 to avoid API limits
    batch_size = 100
    all_vecs: List[np.ndarray] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i: i + batch_size]
        all_vecs.append(embed_fn(batch))
    matrix = np.vstack(all_vecs)  # shape (N, dim)

    dim = matrix.shape[1]
    index = faiss.IndexFlatIP(dim)  # inner product = cosine for normalised vecs
    index.add(matrix)

    faiss.write_index(index, str(store_dir / _INDEX_FILE))
    with open(store_dir / _DOCS_FILE, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)

    num_tables = sum(1 for d in docs if d["metadata"]["type"] == "table")
    num_fields = len(docs) - num_tables
    logger.info(
        "Vector store written to %s  (tables=%d  fields=%d  dim=%d)",
        store_dir, num_tables, num_fields, dim,
    )
    return num_tables, num_fields


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="Stage 05a - Schema Embedder")
    parser.add_argument("--schema", type=Path, required=True, help="Path to final_schema.json")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, config.LOG_LEVEL), format="%(levelname)s: %(message)s")

    with open(args.schema, "r", encoding="utf-8") as f:
        schema_data = json.load(f)

    build_index(schema_data, config.VECTOR_STORE_DIR)
