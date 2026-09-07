-- Infrastructure only: enable the extensions the memory plane needs.
-- Table schema and migrations belong to your application, not this stack.

CREATE EXTENSION IF NOT EXISTS vector;   -- pgvector: embeddings + HNSW
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- keyword half of hybrid retrieval
