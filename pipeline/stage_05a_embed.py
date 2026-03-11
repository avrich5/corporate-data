import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import chromadb
from chromadb.utils import embedding_functions

import config

logger = logging.getLogger(__name__)


class SchemaEmbedder:
    """Builds and manages a ChromaDB vector store of the final schema fields and tables."""
    
    def __init__(self, store_dir: Path = config.VECTOR_STORE_DIR):
        self.store_dir = store_dir
        self.store_dir.mkdir(parents=True, exist_ok=True)
        
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
            embedding_function=self.embedding_fn,
            metadata={"description": "Corporate data schema fields and tables"}
        )

    def _generate_field_text(self, table_name: str, field: Dict[str, Any], relationships: List[Dict[str, Any]]) -> str:
        """Generates embedding text for a specific field based on the Stage 05 spec."""
        field_name = field.get("name", "")
        field_type = field.get("type", "unknown")
        
        text = f"Таблиця {table_name}. Поле: {field_name}. Тип: {field_type}. "
        
        sentinels = field.get("sentinel_values")
        if sentinels:
            if isinstance(sentinels, list):
                sentinels_str = ", ".join(map(str, sentinels))
            else:
                sentinels_str = str(sentinels)
            text += f"Sentinel: {sentinels_str}. "
            
        # Find related tables
        related = []
        for rel in relationships:
            if rel.get("from_table") == table_name and field_name in rel.get("keys", []):
                related.append(f"{rel.get('to_table')} через {', '.join(rel.get('keys', []))}")
            elif rel.get("to_table") == table_name and field_name in rel.get("keys", []):
                related.append(f"{rel.get('from_table')} через {', '.join(rel.get('keys', []))}")
                
        if related:
            text += f"Пов'язана таблиця: {', '.join(related)}."
            
        return text

    def embed_schema(self, schema_data: Dict[str, Any]) -> None:
        """Embeds all tables and fields from the final schema JSON into ChromaDB."""
        tables = schema_data.get("tables", [])
        relationships = schema_data.get("relationships", [])
        
        documents = []
        metadatas = []
        ids = []
        
        logger.info(f"Parsing schema with {len(tables)} tables to embed into {self.store_dir}...")
        
        for table in tables:
            table_name = table.get("name", "")
            if not table_name:
                continue
                
            # Table-level document
            desc = table.get("description", "")
            table_text = f"Таблиця: {table_name}. Опис: {desc}"
            
            documents.append(table_text)
            metadatas.append({
                "type": "table",
                "table_name": table_name,
                "field_name": ""
            })
            ids.append(f"table_{table_name}")
            
            # Field-level documents
            columns = table.get("columns", [])
            for col in columns:
                if col.get("name") in table.get("filter_artifacts", []):
                    continue
                    
                field_name = col.get("name", "")
                if not field_name:
                    continue
                    
                text = self._generate_field_text(table_name, col, relationships)
                
                documents.append(text)
                metadatas.append({
                    "type": "field",
                    "table_name": table_name,
                    "field_name": field_name,
                    "sentinels": str(col.get("sentinel_values", ""))
                })
                ids.append(f"field_{table_name}_{field_name}")
                
        # Incremental logic: delete existing ids if they exist to update
        # We handle this by upserting rather than checking and removing
        if documents:
            logger.info(f"Upserting {len(documents)} documents to vector store...")
            self.collection.upsert(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
            logger.info("Vector store successfully updated.")
        else:
            logger.warning("No documents found to embed.")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Stage 05a - Schema Embedder Build")
    parser.add_argument("--schema", type=Path, required=True, help="Path to final_schema.json")
    args = parser.parse_args()
    
    logging.basicConfig(level=getattr(logging, config.LOG_LEVEL), format="%(levelname)s: %(message)s")
    
    try:
        with open(args.schema, "r", encoding="utf-8") as f:
            schema_data = json.load(f)
            
        embedder = SchemaEmbedder()
        embedder.embed_schema(schema_data)
        
    except Exception as e:
        logger.error(f"Error embedding schema: {e}")
