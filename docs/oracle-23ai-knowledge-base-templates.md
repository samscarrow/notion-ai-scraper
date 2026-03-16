# Oracle 23ai Knowledge Base Templates

These templates add a repo-native starting point for building retrieval-ready knowledge bases on Oracle Database 23ai.

## What was added

- `sql/oracle23ai/knowledge_base_manual.sql`
  - Explicit `documents -> chunks -> embeddings` schema.
  - Best default when you need external embedding providers, per-chunk metadata, or incremental refresh control.
- `sql/oracle23ai/knowledge_base_hybrid.sql`
  - Managed `CREATE HYBRID VECTOR INDEX` path.
  - Best when the corpus already lives in Oracle and your embedding model runs in-database.
- `sql/oracle23ai/query_examples.sql`
  - Exact search, approximate HNSW search, diversified retrieval, manual hybrid fusion, and `DBMS_HYBRID_VECTOR.SEARCH` examples.

## Design choices

### 1. Manual chunk table is the default template
Oracle 23ai supports a fully explicit pipeline: `VECTOR_CHUNKS` for chunking, `VECTOR_EMBEDDING` for in-database embeddings, `VECTOR_DISTANCE` for similarity search, and HNSW/IVF vector indexes for acceleration. That gives better operational control than the hybrid index path when you need:

- external embedding providers,
- per-chunk metadata filters,
- idempotent re-chunking and re-embedding,
- ingestion audit trails.

That is why `knowledge_base_manual.sql` stores both `chunk_text` and `embed_text`.

- `chunk_text` stays citation-clean.
- `embed_text` is allowed to prepend high-signal metadata like title or source type before embedding.

This follows the same basic pattern encouraged by Haystack's metadata-embedding guidance and maps cleanly onto Oracle's explicit vector primitives.

### 2. Hybrid vector indexes stay as a second template, not the only one
Oracle's hybrid vector index is strong when you want Oracle Text and vector retrieval under one managed index, queried through `DBMS_HYBRID_VECTOR.SEARCH`. But Oracle documents this path around in-database ONNX models. That makes it narrower than the explicit schema for teams who want OpenAI, Cohere, or other REST-based embeddings.

### 3. Retrieval quality beats minimal DDL
The template intentionally includes patterns that improve answer quality without making the schema exotic:

- chunk overlap,
- metadata-aware embedding input,
- lexical + semantic fusion,
- result diversification so one document does not dominate the top-N.

The partitioned `FETCH FIRST ... PARTITIONS BY doc_id` example is there for exactly that last case.

## How to use

### Manual template

1. Create or connect to an Oracle 23ai schema.
2. Update the `DEFINE` values in `knowledge_base_manual.sql`.
3. Run the DDL in `knowledge_base_manual.sql`.
4. Load source text into `kb_documents.extracted_text`.
5. Uncomment and adapt the ingest scaffold in `knowledge_base_manual.sql` for each document.
6. Query with examples from `query_examples.sql`.

### Hybrid template

1. Ensure an in-database embedding model is available.
2. Load full document text into `kb_hybrid_documents.doc_text`.
3. Run `knowledge_base_hybrid.sql`.
4. Query via `DBMS_HYBRID_VECTOR.SEARCH`.

## Tradeoffs

| Template | Strengths | Weaknesses | Use when |
| --- | --- | --- | --- |
| Manual chunk store | Flexible, external-provider friendly, filterable, auditable | More schema and pipeline work | Production RAG or multi-source KBs |
| Hybrid vector index | Simple managed index, easy JSON search API | Less flexible, tied more closely to Oracle-managed embedding path | Oracle-native corpora with in-db models |

## FOSS and source credits

- Oracle AI Vector Search Users Guide: `VECTOR_CHUNKS`, `VECTOR_EMBEDDING`, vector indexes, similarity search, and hybrid vector indexes.
- `oracle/langchain-oracle` (UPL-1.0): separation of loader, splitter, embedding, and vector-store concerns for Oracle-backed RAG.
- Haystack docs (Apache-2.0): clean/split preprocessing, embedding selected metadata, and grouping/diversification patterns.

## Sources

- Oracle docs: https://docs.oracle.com/en/database/oracle/oracle-database/23/vecse/overview-ai-vector-search.html
- Oracle docs: https://docs.oracle.com/en/database/oracle/oracle-database/23/vecse/use-sql-functions-vector-operations.html
- Oracle docs: https://docs.oracle.com/en/database/oracle/oracle-database/23/vecse/create-vector-indexes-and-hybrid-vector-indexes.html
- Oracle docs: https://docs.oracle.com/en/database/oracle/oracle-database/23/vecse/search-data-using-similarity-search.html
- Oracle docs: https://docs.oracle.com/en/database/oracle/oracle-database/23/vecse/perform-chunking-embedding-oracle-ai-database.html
- `oracle/langchain-oracle`: https://github.com/oracle/langchain-oracle
- Haystack DocumentSplitter: https://docs.haystack.deepset.ai/docs/documentsplitter
- Haystack embedding metadata: https://docs.haystack.deepset.ai/docs/embedders#embedding-metadata
- Haystack MetaFieldGroupingRanker: https://docs.haystack.deepset.ai/docs/metafieldgroupingranker
