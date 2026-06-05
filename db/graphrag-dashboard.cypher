// ═══════════════════════════════════════════════════════════════════════════
//  GRAPHRAG DASHBOARD — Aura Visualization Queries
//  Database: GraphRAG_MVP  |  Schema: (Document)-[HAS_CHUNK]->(Chunk), [REFERS_TO]
//
//  📖 Reference: https://neo4j.com/docs/aura/dashboards/visualizations/
//
//  Mỗi query bên dưới được thiết kế cho 1 Aura dashboard card type cụ thể.
//  Shape của RETURN được tối ưu theo yêu cầu của từng visualization.
//
//  HOW TO USE:
//  1. Aura Console → Dashboards → New dashboard → Create from scratch
//  2. Add card → chọn đúng "chart type" như chú thích [TYPE: ...] ở mỗi query
//  3. Edit Cypher → paste query → Save
// ═══════════════════════════════════════════════════════════════════════════


// ╔═════════════════════════════════════════════════════════════════════════╗
// ║  PAGE 1: OVERVIEW                                                       ║
// ╚═════════════════════════════════════════════════════════════════════════╝

// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Single value]  Total Documents
// Card type: Single value / Stat
// Shape: RETURN <single_number>
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
RETURN count(d) AS Documents;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Single value]  Total Chunks
// ───────────────────────────────────────────────────────────────────────────
MATCH (c:Chunk)
RETURN count(c) AS Chunks;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Single value]  Total Relationships
// ───────────────────────────────────────────────────────────────────────────
MATCH ()-[r]->()
RETURN count(r) AS Relationships;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Single value]  Avg Chunks per Document
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
OPTIONAL MATCH (d)-[:HAS_CHUNK]->(c:Chunk)
WITH d, count(c) AS chunks
RETURN round(avg(chunks), 1) AS AvgChunksPerDoc;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Pie chart]  Node distribution by label
// Shape: Category (text), Value (number)
// ───────────────────────────────────────────────────────────────────────────
MATCH (n)
RETURN labels(n)[0] AS Label, count(*) AS Count
ORDER BY Count DESC;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Pie chart]  Relationship distribution by type
// ───────────────────────────────────────────────────────────────────────────
MATCH ()-[r]->()
RETURN type(r) AS RelType, count(*) AS Count
ORDER BY Count DESC;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Bar chart]  Top 10 Documents by chunk count
// Shape: Category (text) on X-axis, Value (number) on Y-axis
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)-[:HAS_CHUNK]->(c:Chunk)
RETURN coalesce(d.title, d.doc_id) AS Document, count(c) AS Chunks
ORDER BY Chunks DESC
LIMIT 10;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Bar chart]  Documents per issuing body
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
WHERE d.issuing_body IS NOT NULL
RETURN d.issuing_body AS IssuingBody, count(*) AS Documents
ORDER BY Documents DESC
LIMIT 15;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Bar chart]  Documents by doc_type
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
RETURN coalesce(d.doc_type, 'unknown') AS DocType, count(*) AS Count
ORDER BY Count DESC;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Bar chart]  Chunks per document — histogram (bucketed)
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
OPTIONAL MATCH (d)-[:HAS_CHUNK]->(c:Chunk)
WITH d, count(c) AS chunkCount
WITH
  CASE
    WHEN chunkCount = 0   THEN '0'
    WHEN chunkCount <= 5  THEN '1-5'
    WHEN chunkCount <= 20 THEN '6-20'
    WHEN chunkCount <= 50 THEN '21-50'
    WHEN chunkCount <= 100 THEN '51-100'
    ELSE '100+'
  END AS Bucket,
  d
RETURN Bucket, count(d) AS Documents
ORDER BY
  CASE Bucket
    WHEN '0' THEN 0
    WHEN '1-5' THEN 1
    WHEN '6-20' THEN 2
    WHEN '21-50' THEN 3
    WHEN '51-100' THEN 4
    ELSE 5
  END;


// ╔═════════════════════════════════════════════════════════════════════════╗
// ║  PAGE 2: DOCUMENTS                                                      ║
// ╚═════════════════════════════════════════════════════════════════════════╝

// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Table]  All documents with metadata + chunk counts
// Shape: Multiple columns; Aura renders pagination automatically
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
OPTIONAL MATCH (d)-[:HAS_CHUNK]->(c:Chunk)
RETURN
  coalesce(d.title, '—')         AS Title,
  coalesce(d.issuing_body, '—')  AS IssuingBody,
  coalesce(d.doc_type, '—')      AS Type,
  coalesce(d.status, '—')        AS Status,
  count(c)                       AS Chunks
ORDER BY Chunks DESC
LIMIT 200;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Table]  Documents missing required metadata
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
WHERE d.title IS NULL OR d.issuing_body IS NULL OR d.doc_type IS NULL
RETURN
  d.doc_id                                AS DocId,
  CASE WHEN d.title IS NULL THEN '✗' ELSE '✓' END         AS Title,
  CASE WHEN d.issuing_body IS NULL THEN '✗' ELSE '✓' END  AS Body,
  CASE WHEN d.doc_type IS NULL THEN '✗' ELSE '✓' END      AS Type
LIMIT 100;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Table]  Recently updated documents
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
WHERE d.updated_at IS NOT NULL
RETURN
  coalesce(d.title, d.doc_id) AS Document,
  d.issuing_body              AS IssuingBody,
  d.updated_at                AS LastUpdated
ORDER BY d.updated_at DESC
LIMIT 25;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Pie chart]  Documents by status
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
RETURN coalesce(d.status, 'unknown') AS Status, count(*) AS Count
ORDER BY Count DESC;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Bar chart]  Orphan documents per issuing body (no chunks)
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
WHERE NOT (d)-[:HAS_CHUNK]->()
RETURN coalesce(d.issuing_body, 'unknown') AS IssuingBody, count(*) AS Orphans
ORDER BY Orphans DESC
LIMIT 15;


// ╔═════════════════════════════════════════════════════════════════════════╗
// ║  PAGE 3: CHUNKS                                                         ║
// ╚═════════════════════════════════════════════════════════════════════════╝

// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Single value]  Total chunks
// ───────────────────────────────────────────────────────────────────────────
MATCH (c:Chunk)
RETURN count(c) AS TotalChunks;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Single value]  Avg token count per chunk
// ───────────────────────────────────────────────────────────────────────────
MATCH (c:Chunk)
WHERE c.token_count IS NOT NULL
RETURN round(avg(c.token_count)) AS AvgTokens;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Single value]  Max token count
// ───────────────────────────────────────────────────────────────────────────
MATCH (c:Chunk)
WHERE c.token_count IS NOT NULL
RETURN max(c.token_count) AS MaxTokens;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Bar chart]  Token count distribution (histogram)
// ───────────────────────────────────────────────────────────────────────────
MATCH (c:Chunk)
WHERE c.token_count IS NOT NULL
WITH
  CASE
    WHEN c.token_count <= 100  THEN '0-100'
    WHEN c.token_count <= 250  THEN '101-250'
    WHEN c.token_count <= 500  THEN '251-500'
    WHEN c.token_count <= 1000 THEN '501-1000'
    WHEN c.token_count <= 2000 THEN '1001-2000'
    ELSE '2000+'
  END AS Bucket
RETURN Bucket, count(*) AS Chunks
ORDER BY
  CASE Bucket
    WHEN '0-100' THEN 0
    WHEN '101-250' THEN 1
    WHEN '251-500' THEN 2
    WHEN '501-1000' THEN 3
    WHEN '1001-2000' THEN 4
    ELSE 5
  END;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Bar chart]  Top 15 chapters by chunk count
// ───────────────────────────────────────────────────────────────────────────
MATCH (c:Chunk)
WHERE c.chapter IS NOT NULL
RETURN c.chapter AS Chapter, count(*) AS Chunks
ORDER BY Chunks DESC
LIMIT 15;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Table]  Chunks with highest token count (potential too-long chunks)
// ───────────────────────────────────────────────────────────────────────────
MATCH (c:Chunk)
WHERE c.token_count IS NOT NULL
OPTIONAL MATCH (d:Document)-[:HAS_CHUNK]->(c)
RETURN
  c.chunk_uid                         AS ChunkUid,
  coalesce(d.title, '—')              AS Document,
  c.article                           AS Article,
  c.token_count                       AS Tokens,
  substring(c.text, 0, 100) + '...'   AS Preview
ORDER BY c.token_count DESC
LIMIT 25;


// ╔═════════════════════════════════════════════════════════════════════════╗
// ║  PAGE 4: REFERENCES                                                     ║
// ╚═════════════════════════════════════════════════════════════════════════╝

// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Single value]  Total REFERS_TO edges
// ───────────────────────────────────────────────────────────────────────────
MATCH ()-[r:REFERS_TO]->()
RETURN count(r) AS Refs;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Bar chart]  Most referenced documents (incoming REFERS_TO)
// ───────────────────────────────────────────────────────────────────────────
MATCH (target:Document)<-[:REFERS_TO]-(src)
RETURN
  coalesce(target.title, target.doc_id) AS Document,
  count(src)                             AS Incoming
ORDER BY Incoming DESC
LIMIT 10;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Bar chart]  Top "referencing" documents (outgoing REFERS_TO)
// ───────────────────────────────────────────────────────────────────────────
MATCH (src:Document)-[:REFERS_TO]->(target)
RETURN
  coalesce(src.title, src.doc_id) AS Document,
  count(target)                    AS Outgoing
ORDER BY Outgoing DESC
LIMIT 10;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Pie chart]  Reference relation_type distribution
// ───────────────────────────────────────────────────────────────────────────
MATCH ()-[r:REFERS_TO]->()
WHERE r.relation_type IS NOT NULL
RETURN r.relation_type AS RelationType, count(*) AS Count
ORDER BY Count DESC;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Table]  Cross-document reference edges
// ───────────────────────────────────────────────────────────────────────────
MATCH (src:Document)-[r:REFERS_TO]->(tgt:Document)
RETURN
  coalesce(src.title, src.doc_id)         AS Source,
  coalesce(r.relation_type, '—')          AS RelType,
  coalesce(tgt.title, tgt.doc_id)         AS Target
LIMIT 100;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Bar chart]  Document centrality (incoming + outgoing refs)
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
OPTIONAL MATCH (d)-[:REFERS_TO]->(out)
WITH d, count(out) AS outgoing
OPTIONAL MATCH (d)<-[:REFERS_TO]-(inc)
WITH d, outgoing, count(inc) AS incoming
WHERE outgoing + incoming > 0
RETURN
  coalesce(d.title, d.doc_id) AS Document,
  incoming + outgoing         AS Centrality
ORDER BY Centrality DESC
LIMIT 15;


// ╔═════════════════════════════════════════════════════════════════════════╗
// ║  PAGE 5: GRAPH VIEWS                                                    ║
// ╚═════════════════════════════════════════════════════════════════════════╝

// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Graph]  Sample subgraph: 5 documents and their chunks
// Shape: RETURN nodes and relationships (NOT scalars)
// Aura render: drag-and-drop, force-based or hierarchical layout
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)-[:HAS_CHUNK]->(c:Chunk)
WITH d LIMIT 5
MATCH (d)-[r:HAS_CHUNK]->(c:Chunk)
RETURN d, r, c
LIMIT 100;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Graph]  REFERS_TO network among Documents
// ───────────────────────────────────────────────────────────────────────────
MATCH p = (a:Document)-[:REFERS_TO]->(b:Document)
RETURN p
LIMIT 75;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Graph]  Top hub document + neighborhood
// First find the hub, then expand 2 hops out
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
OPTIONAL MATCH (d)-[:REFERS_TO]-()
WITH d, count(*) AS degree
ORDER BY degree DESC
LIMIT 1
MATCH p = (d)-[*1..2]-()
RETURN p
LIMIT 75;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Graph]  Filtered exploration — đổi keyword bên dưới
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
WHERE toLower(d.title) CONTAINS toLower('luật')
WITH d LIMIT 3
OPTIONAL MATCH (d)-[r:HAS_CHUNK]->(c:Chunk)
RETURN d, r, c
LIMIT 80;


// ╔═════════════════════════════════════════════════════════════════════════╗
// ║  PAGE 6: HIERARCHICAL VIEWS (Sunburst / Treemap if supported)           ║
// ╚═════════════════════════════════════════════════════════════════════════╝

// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Sunburst / Treemap]  IssuingBody → DocType → Document
// Shape: Path (list of strings) + Value (number)
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)-[:HAS_CHUNK]->(c:Chunk)
WITH
  coalesce(d.issuing_body, 'Unknown') AS body,
  coalesce(d.doc_type, 'Unknown')     AS type,
  coalesce(d.title, d.doc_id)         AS title,
  count(c)                            AS chunks
RETURN [body, type, title] AS Path, chunks AS Value
ORDER BY chunks DESC
LIMIT 50;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Sunburst / Treemap]  IssuingBody → DocType (2 levels)
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
WITH
  coalesce(d.issuing_body, 'Unknown') AS body,
  coalesce(d.doc_type, 'Unknown')     AS type
RETURN [body, type] AS Path, count(*) AS Value
ORDER BY Value DESC;


// ╔═════════════════════════════════════════════════════════════════════════╗
// ║  PAGE 7: DATA QUALITY                                                   ║
// ╚═════════════════════════════════════════════════════════════════════════╝

// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Single value]  % Documents with full metadata
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
WITH count(d) AS total,
     count(CASE WHEN d.title IS NOT NULL
                  AND d.issuing_body IS NOT NULL
                  AND d.doc_type IS NOT NULL
                THEN 1 END) AS complete
RETURN round(toFloat(complete) / total * 100, 1) AS PctComplete;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Single value]  Orphan chunks (no parent Document)
// ───────────────────────────────────────────────────────────────────────────
MATCH (c:Chunk)
WHERE NOT (:Document)-[:HAS_CHUNK]->(c)
RETURN count(c) AS OrphanChunks;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Single value]  Isolated chunks (not in REFERS_TO network)
// ───────────────────────────────────────────────────────────────────────────
MATCH (c:Chunk)
WHERE NOT (c)-[:REFERS_TO]-() AND NOT ()-[:REFERS_TO]->(c)
RETURN count(c) AS IsolatedChunks;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Bar chart]  Property coverage (% non-null per document property)
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
WITH count(d) AS total,
     count(d.title)        AS title,
     count(d.issuing_body) AS body,
     count(d.doc_type)     AS dType,
     count(d.status)       AS status,
     count(d.updated_at)   AS updated
UNWIND [
  {Property: 'title',        Coverage: round(toFloat(title)   / total * 100, 1)},
  {Property: 'issuing_body', Coverage: round(toFloat(body)    / total * 100, 1)},
  {Property: 'doc_type',     Coverage: round(toFloat(dType)   / total * 100, 1)},
  {Property: 'status',       Coverage: round(toFloat(status)  / total * 100, 1)},
  {Property: 'updated_at',   Coverage: round(toFloat(updated) / total * 100, 1)}
] AS row
RETURN row.Property AS Property, row.Coverage AS Coverage
ORDER BY Coverage DESC;


// ───────────────────────────────────────────────────────────────────────────
// [TYPE: Table]  Duplicate doc_id (potential data issue)
// ───────────────────────────────────────────────────────────────────────────
MATCH (d:Document)
WHERE d.doc_id IS NOT NULL
WITH d.doc_id AS DocId, collect(d) AS docs
WHERE size(docs) > 1
RETURN DocId, size(docs) AS Duplicates
ORDER BY Duplicates DESC
LIMIT 50;


// ═══════════════════════════════════════════════════════════════════════════
//  📌 NOTES về Aura Dashboard visualization shapes
// ───────────────────────────────────────────────────────────────────────────
//  • Single value     : RETURN <number>             (1 row, 1 numeric column)
//  • Bar chart        : RETURN <text>, <number>     (Category + Value)
//  • Pie chart        : RETURN <text>, <number>     (Category + Value)
//  • Line chart       : RETURN <date/x>, <number>   (X-axis + Value)
//  • Table            : RETURN any columns          (always works)
//  • Graph            : RETURN nodes/rels/paths     (n, r, m or path p)
//  • Sunburst/Treemap : RETURN [list_of_strings], <number>
//                       (Path hierarchy + Value)
//
//  ⚠ LIMIT là bắt buộc cho Graph và recommended cho Table (perf).
//  ⚠ Aura Console KHÔNG hỗ trợ :param — dùng WITH 'value' AS x thay thế.
// ═══════════════════════════════════════════════════════════════════════════
