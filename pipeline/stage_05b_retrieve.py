"""Stage 05b — Schema Retriever.

Loads the FAISS index built by stage_05a_embed and performs semantic search
to return a filtered sub-schema compatible with QueryGenerator.

Usage:
  python -m pipeline.stage_05b_retrieve --query "топ категорій по доходу"
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import faiss
import numpy as np

import config

logger = logging.getLogger(__name__)

_INDEX_FILE = "schema_index.faiss"
_DOCS_FILE = "schema_docs.json"


class SchemaRetriever:
    """Retrieves relevant schema fields from a FAISS vector store."""

    def __init__(self, store_dir: Path = config.VECTOR_STORE_DIR):
        self.store_dir = store_dir
        index_path = store_dir / _INDEX_FILE
        docs_path = store_dir / _DOCS_FILE

        if not index_path.exists() or not docs_path.exists():
            raise FileNotFoundError(
                f"Vector store not found at {store_dir}. "
                "Run: python -m pipeline.stage_05a_embed --schema <path>"
            )

        self.index = faiss.read_index(str(index_path))
        with open(docs_path, "r", encoding="utf-8") as f:
            self.docs: List[Dict[str, Any]] = json.load(f)

        logger.info("Loaded vector store: %d vectors from %s", self.index.ntotal, store_dir)
        self._embed_fn = self._build_embed_fn()

    def _build_embed_fn(self):
        """Builds the same embedding function used during indexing."""
        if config.EMBEDDING_PROVIDER.lower() == "local":
            from sentence_transformers import SentenceTransformer
            model_name = (config.EMBEDDING_MODEL
                          if config.EMBEDDING_MODEL != "text-embedding-3-small"
                          else "all-MiniLM-L6-v2")
            model = SentenceTransformer(model_name)

            def _local(texts: List[str]) -> np.ndarray:
                vecs = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
                return np.array(vecs, dtype=np.float32)

            return _local
        else:
            from openai import OpenAI
            if not config.OPENAI_API_KEY:
                raise ValueError("OPENAI_API_KEY must be set when EMBEDDING_PROVIDER='openai'")
            client = OpenAI(api_key=config.OPENAI_API_KEY)
            model_name = config.EMBEDDING_MODEL

            def _openai(texts: List[str]) -> np.ndarray:
                resp = client.embeddings.create(input=texts, model=model_name)
                vecs = [item.embedding for item in resp.data]
                arr = np.array(vecs, dtype=np.float32)
                norms = np.linalg.norm(arr, axis=1, keepdims=True)
                return arr / np.maximum(norms, 1e-9)

            return _openai

    def retrieve(
        self,
        query: str,
        top_k: int = config.RETRIEVAL_TOP_K,
        min_score: float = config.RETRIEVAL_MIN_SCORE,
    ) -> Dict[str, Any]:
        """Searches vector store and returns a filtered sub-schema dict.

        Args:
            query: Natural language query string.
            top_k: Maximum number of results to retrieve.
            min_score: Minimum cosine similarity (0-1) to include a result.

        Returns:
            Dict with key "tables" — list of table dicts with filtered columns.
        """
        query_vec = self._embed_fn([query])  # shape (1, dim)

        n_results = min(top_k, self.index.ntotal)
        if n_results == 0:
            logger.warning("Vector store is empty.")
            return {"tables": []}

        scores, indices = self.index.search(query_vec, n_results)
        scores = scores[0]   # shape (n_results,)
        indices = indices[0]

        # Build filtered sub-schema grouped by table
        table_map: Dict[str, Dict[str, Any]] = {}
        retrieved_count = 0

        for score, idx in zip(scores, indices):
            if idx < 0:
                continue  # FAISS pads with -1 when fewer results exist
            if float(score) < min_score:
                continue

            doc = self.docs[idx]
            meta = doc["metadata"]
            t_name = meta["table_name"]
            f_name = meta["field_name"]
            m_type = meta["type"]

            if t_name not in table_map:
                table_map[t_name] = {"name": t_name, "columns": []}

            if m_type == "field" and f_name:
                already = any(c["field_name"] == f_name for c in table_map[t_name]["columns"])
                if not already:
                    col: Dict[str, Any] = {"field_name": f_name}
                    sv = meta.get("sentinels", "")
                    if sv and sv not in ("None", "[]", ""):
                        col["sentinel_values"] = sv
                    table_map[t_name]["columns"].append(col)
                    retrieved_count += 1

        result = {"tables": list(table_map.values())}
        logger.info(
            "Retrieved %d fields across %d tables (min_score=%.2f)",
            retrieved_count, len(table_map), min_score,
        )
        return result


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="Stage 05b - Schema Retriever")
    parser.add_argument("--query", type=str, required=True, help="Natural language query")
    parser.add_argument("--top-k", type=int, default=config.RETRIEVAL_TOP_K)
    parser.add_argument("--min-score", type=float, default=config.RETRIEVAL_MIN_SCORE)
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, config.LOG_LEVEL), format="%(levelname)s: %(message)s")

    retriever = SchemaRetriever()
    result = retriever.retrieve(query=args.query, top_k=args.top_k, min_score=args.min_score)

    print("\n--- RETRIEVED SUB-SCHEMA ---")
    print(json.dumps(result, indent=2, ensure_ascii=False))
