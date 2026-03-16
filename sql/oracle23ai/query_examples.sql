-- Query cookbook for the Oracle 23ai knowledge-base templates.
-- Replace bind variables and substitution variables as needed.

DEFINE DISTANCE_METRIC = COSINE
DEFINE TARGET_ACCURACY = 90
DEFINE INDB_MODEL = doc_model

--------------------------------------------------------------------------------
-- 1. Exact semantic search with metadata filters.
--------------------------------------------------------------------------------
WITH q AS (
  SELECT VECTOR_EMBEDDING(&INDB_MODEL USING :query_text AS data) AS query_vec
  FROM dual
)
SELECT
  c.doc_id,
  c.chunk_id,
  d.title,
  d.source_uri,
  VECTOR_DISTANCE(c.embedding, q.query_vec, &DISTANCE_METRIC) AS distance,
  DBMS_LOB.SUBSTR(c.chunk_text, 600, 1) AS preview
FROM kb_chunks c
JOIN kb_documents d ON d.doc_id = c.doc_id
CROSS JOIN q
WHERE JSON_VALUE(d.metadata, '$.product' RETURNING VARCHAR2(128)) = :product
ORDER BY distance
FETCH FIRST 10 ROWS ONLY;

--------------------------------------------------------------------------------
-- 2. Approximate semantic search using the HNSW index.
--------------------------------------------------------------------------------
WITH q AS (
  SELECT VECTOR_EMBEDDING(&INDB_MODEL USING :query_text AS data) AS query_vec
  FROM dual
)
SELECT
  c.doc_id,
  c.chunk_id,
  VECTOR_DISTANCE(c.embedding, q.query_vec, &DISTANCE_METRIC) AS distance,
  DBMS_LOB.SUBSTR(c.chunk_text, 400, 1) AS preview
FROM kb_chunks c
CROSS JOIN q
ORDER BY distance
FETCH APPROXIMATE FIRST 25 ROWS ONLY WITH TARGET ACCURACY &TARGET_ACCURACY;

--------------------------------------------------------------------------------
-- 3. Diversify results so one document does not monopolize the top-N.
--    Oracle 23ai supports partitioned FETCH FIRST for multi-vector style retrieval.
--------------------------------------------------------------------------------
WITH q AS (
  SELECT VECTOR_EMBEDDING(&INDB_MODEL USING :query_text AS data) AS query_vec
  FROM dual
)
SELECT
  c.doc_id,
  c.chunk_id,
  VECTOR_DISTANCE(c.embedding, q.query_vec, &DISTANCE_METRIC) AS distance,
  DBMS_LOB.SUBSTR(c.chunk_text, 400, 1) AS preview
FROM kb_chunks c
CROSS JOIN q
ORDER BY distance
FETCH FIRST 10 PARTITIONS BY doc_id, 2 ROWS ONLY;

--------------------------------------------------------------------------------
-- 4. Manual hybrid search via Reciprocal Rank Fusion.
--    Inspired by Oracle Text + vector search and typical RAG rank fusion patterns.
--------------------------------------------------------------------------------
WITH q AS (
  SELECT VECTOR_EMBEDDING(&INDB_MODEL USING :semantic_query AS data) AS query_vec
  FROM dual
),
vector_hits AS (
  SELECT
    c.chunk_id,
    c.doc_id,
    ROW_NUMBER() OVER (
      ORDER BY VECTOR_DISTANCE(c.embedding, q.query_vec, &DISTANCE_METRIC), c.chunk_id
    ) AS vector_rank
  FROM kb_chunks c
  CROSS JOIN q
  FETCH APPROXIMATE FIRST 100 ROWS ONLY WITH TARGET ACCURACY &TARGET_ACCURACY
),
text_hits AS (
  SELECT
    c.chunk_id,
    c.doc_id,
    ROW_NUMBER() OVER (ORDER BY SCORE(1) DESC, c.chunk_id) AS text_rank
  FROM kb_chunks c
  WHERE CONTAINS(c.chunk_text, :keyword_query, 1) > 0
  FETCH FIRST 100 ROWS ONLY
),
fused AS (
  SELECT chunk_id, doc_id, SUM(score_component) AS fused_score
  FROM (
    SELECT chunk_id, doc_id, 1 / (60 + vector_rank) AS score_component FROM vector_hits
    UNION ALL
    SELECT chunk_id, doc_id, 1 / (60 + text_rank) AS score_component FROM text_hits
  )
  GROUP BY chunk_id, doc_id
)
SELECT
  f.doc_id,
  f.chunk_id,
  f.fused_score,
  d.title,
  d.source_uri,
  DBMS_LOB.SUBSTR(c.chunk_text, 500, 1) AS preview
FROM fused f
JOIN kb_chunks c ON c.chunk_id = f.chunk_id
JOIN kb_documents d ON d.doc_id = f.doc_id
ORDER BY f.fused_score DESC, f.chunk_id
FETCH FIRST 10 ROWS ONLY;

--------------------------------------------------------------------------------
-- 5. Hybrid vector index search returning Oracle-managed JSON.
--------------------------------------------------------------------------------
SELECT JSON_SERIALIZE(
         DBMS_HYBRID_VECTOR.SEARCH(
           JSON(
             '{"hybrid_index_name":"KB_HYBRID_IDX","vector":{"search_text":"' ||
             REPLACE(:query_text, '"', '\\"') ||
             '"},"text":{"contains":"' || REPLACE(:keyword_query, '"', '\\"') ||
             '"},"return":{"topN":10}}'
           )
         ) PRETTY
       ) AS search_result
FROM dual;
