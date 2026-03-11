import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import chromadb
from chromadb.utils import embedding_functions

import config

logger = logging.getLogger(__name__)


class SchemaRetriever:
    """Retrieves context from ChromaDB based on natural language queries."""

    def __init__(self, store_dir: Path = config.VECTOR_STORE_DIR):
        self.store_dir = store_dir
        if not self.store_dir.exists():
            logger.warning(f"Vector store directory {self.store_dir} does not exist. Call embedder first.")
            
        self.client = chromadb.PersistentClient(path=str(self.store_dir))
        
        if config.EMBEDDING_PROVIDER.lower() == "local":
            self.embedding_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
                model_name=config.EMBEDDING_MODEL
            )
        else:
            api_key = config.OPENAI_API_KEY
            if not api_key:
                raise ValueError("OPENAI_API_KEY must be set when EMBEDDING_PROVIDER is 'openai'")
            self.embedding_fn = embedding_functions.OpenAIEmbeddingFunction(
                api_key=api_key,
                model_name=config.EMBEDDING_MODEL
            )
            
        self.collection = self.client.get_or_create_collection(
            name="schema_catalog",
            embedding_function=self.embedding_fn
        )

    def retrieve(self, query: str, top_k: int = config.RETRIEVAL_TOP_K, min_score: float = config.RETRIEVAL_MIN_SCORE) -> Dict[str, Any]:
        """Performs semantic search to find top-K schema fields and constructs a filtered schema sub-dictionary."""
        
        logger.info(f"Retrieving top {top_k} results for query: '{query}'")
        results = self.collection.query(
            query_texts=[query],
            n_results=top_k,
            include=["metadatas", "distances", "documents"]
        )
        
        if not results["ids"] or not results["ids"][0]:
            return {"tables": []}
            
        metadatas = results["metadatas"][0]
        distances = results["distances"][0]
        
        # Convert distance to a normalized similarity score (for cosine/l2 proxy)
        # Assuming L2 distance by default in Chroma. Score = 1 / (1 + distance) roughly or customized.
        # But we will use the raw distance threshold logic if needed. For now treating min_score loosely 
        # as a filter. Since Chroma uses L2 distance default, smaller is better.
        # Wait, the prompt says "score >= min_score". If it's cosine similarity, higher is better.
        # Openai embedding function normalizes so L2 == Cosine distance.
        # Cosine similarity = 1 - (L2^2 / 2).
        
        retrieved_items = []
        for meta, dist in zip(metadatas, distances):
            score = 1 - (dist / 2.0)
            if score >= min_score:
                retrieved_items.append({
                    "meta": meta,
                    "score": score
                })
                
        # Group by table to generate a synthetic sub-schema
        filtered_schema: Dict[str, Any] = {"tables": []}
        table_map = {}
        
        for item in retrieved_items:
            t_name = item["meta"].get("table_name")
            f_name = item["meta"].get("field_name")
            m_type = item["meta"].get("type")
            
            if t_name not in table_map:
                table_map[t_name] = {"name": t_name, "columns": []}
                
            if m_type == "field" and f_name:
                # check if column already added
                if not any(c["name"] == f_name for c in table_map[t_name]["columns"]):
                    sentinels = item["meta"].get("sentinels", "")
                    col = {"name": f_name}
                    if sentinels and sentinels != "None" and sentinels != "[]":
                        col["sentinel_values"] = sentinels
                    table_map[t_name]["columns"].append(col)
                    
        filtered_schema["tables"] = list(table_map.values())
        return filtered_schema


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Stage 05b - Schema Retriever")
    parser.add_argument("--query", type=str, required=True, help="Natural language query")
    args = parser.parse_args()
    
    logging.basicConfig(level=getattr(logging, config.LOG_LEVEL), format="%(levelname)s: %(message)s")
    
    try:
        retriever = SchemaRetriever()
        filtered = retriever.retrieve(query=args.query)
        
        print("\n--- RETRIEVED SUB-SCHEMA ---")
        print(json.dumps(filtered, indent=2, ensure_ascii=False))
        
    except Exception as e:
        logger.error(f"Error retrieving schema: {e}")
