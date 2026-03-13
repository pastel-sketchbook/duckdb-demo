"""Full-text search — find documents by keyword relevance using BM25 scoring.

DuckDB's `fts` (full-text search) extension lets you create inverted indexes
on text columns and rank results using the BM25 algorithm — the same ranking
function used by search engines.  No external server needed.

Covers: installing the fts extension, PRAGMA create_fts_index, match_bm25()
scoring, multi-word queries, column-specific indexing, combining FTS with
regular SQL filters, and re-indexing after data changes.

Run with:
    uv run python examples/66_full_text_search.py
"""

from __future__ import annotations

import duckdb

conn = duckdb.connect()

# =============================================================================
# 1. Install and load the fts extension
# =============================================================================
# The fts extension ships with DuckDB but must be explicitly installed and
# loaded before first use.  Subsequent sessions only need LOAD.
print("=== 1. Install and Load the FTS Extension ===")

conn.execute("INSTALL fts;")
conn.execute("LOAD fts;")
print("  fts extension loaded successfully.\n")

# =============================================================================
# 2. Create a documents table with realistic tech-topic content
# =============================================================================
# Each document has an id, title, body, and a category tag.  The body text
# is long enough (2-3 sentences) to make BM25 term-frequency scoring meaningful.
print("=== 2. Create Documents Table ===")

conn.execute(
    """
    CREATE TABLE docs (
        id       INTEGER PRIMARY KEY,
        title    VARCHAR,
        body     VARCHAR,
        category VARCHAR
    );
    """
)

conn.execute(
    """
    INSERT INTO docs VALUES
    (1,
     'Getting Started with DuckDB',
     'DuckDB is an in-process analytical database designed for fast OLAP queries. '
     'It requires no server setup and runs entirely inside your application process. '
     'DuckDB supports standard SQL and integrates seamlessly with Python and R.',
     'database'),

    (2,
     'Python for Data Analysis',
     'Python is the most popular language for data science and analytics workflows. '
     'Libraries like pandas and polars make it easy to load, transform, and analyze '
     'large datasets without writing SQL queries.',
     'python'),

    (3,
     'Introduction to SQL Window Functions',
     'Window functions allow you to perform calculations across a set of rows related '
     'to the current row without collapsing them into a single output. '
     'Common window functions include ROW_NUMBER, RANK, LAG, LEAD, and SUM OVER.',
     'sql'),

    (4,
     'Building Analytics Dashboards',
     'Analytics dashboards combine SQL queries with visualization libraries to present '
     'key business metrics in real time. Tools like Plotly, Grafana, and Metabase '
     'make it easy to build interactive dashboards from analytical database queries.',
     'analytics'),

    (5,
     'Understanding Database Indexes',
     'Database indexes speed up query performance by creating sorted data structures '
     'that allow the query engine to locate rows without scanning the entire table. '
     'Common index types include B-tree, hash, and full-text search indexes.',
     'database'),

    (6,
     'Pandas vs Polars for DataFrame Operations',
     'Pandas has been the dominant Python DataFrame library for over a decade. '
     'Polars is a newer alternative written in Rust that offers significantly faster '
     'performance on large datasets thanks to lazy evaluation and multi-threaded execution.',
     'python'),

    (7,
     'Advanced SQL Aggregations and GROUP BY',
     'SQL aggregation functions like SUM, AVG, COUNT, and MAX collapse multiple rows '
     'into summary statistics. The GROUP BY clause partitions data into groups before '
     'applying these aggregations, enabling powerful analytical queries.',
     'sql'),

    (8,
     'Real-Time Analytics with Streaming Data',
     'Streaming analytics processes data as it arrives rather than in batch mode. '
     'Technologies like Apache Kafka and Apache Flink enable real-time data pipelines. '
     'Many analytical databases now support ingesting streaming data for live queries.',
     'analytics'),

    (9,
     'Query Optimization in Analytical Databases',
     'Query optimization involves rewriting SQL statements and choosing execution plans '
     'that minimize resource usage. Analytical databases use columnar storage, predicate '
     'pushdown, and parallel execution to achieve fast query performance.',
     'database'),

    (10,
     'Python Type Hints and Static Analysis',
     'Type hints let Python developers annotate function signatures with expected types. '
     'Static analysis tools like mypy and ty check these annotations at development time '
     'to catch bugs before the code runs, improving code quality and readability.',
     'python');
    """
)

conn.sql("SELECT id, title, category FROM docs ORDER BY id").show()

# =============================================================================
# 3. Create a full-text search index
# =============================================================================
# PRAGMA create_fts_index(table, doc_id_column, column1, column2, ...)
# This builds an inverted index on the specified text columns.  The second
# argument identifies the unique document ID used to join results back.
print("=== 3. Create a Full-Text Search Index ===")

conn.execute(
    """
    PRAGMA create_fts_index(
        'docs',       -- table name
        'id',         -- document id column
        'title',      -- columns to index
        'body'
    );
    """
)

print("  FTS index created on docs(title, body).\n")

# =============================================================================
# 4. Basic search with match_bm25()
# =============================================================================
# fts_main_<table>.match_bm25(id_column, 'search terms') returns a relevance
# score for each matching document.  Lower (more negative) scores = higher
# relevance.  Documents that don't match return NULL.
print("=== 4. Basic Search — single term ===")

conn.sql(
    """
    SELECT
        d.id,
        d.title,
        d.category,
        score
    FROM docs d
    JOIN (
        SELECT *, fts_main_docs.match_bm25(id, 'database') AS score
        FROM docs
    ) s ON d.id = s.id
    WHERE score IS NOT NULL
    ORDER BY score
    """
).show()
# Documents mentioning "database" are ranked — those with more occurrences
# or where the term is more prominent score higher (lower numeric value).

# A simpler single-table approach using a subquery:
print("  Simpler form using a WHERE filter:")
conn.sql(
    """
    SELECT
        id,
        title,
        fts_main_docs.match_bm25(id, 'database') AS score
    FROM docs
    WHERE score IS NOT NULL
    ORDER BY score
    """
).show()

# =============================================================================
# 5. Multi-word search — phrases and multiple terms
# =============================================================================
# You can pass multiple words.  BM25 scores documents containing any of the
# terms, giving higher scores to documents that contain more of them.
print("=== 5. Multi-Word Search ===")

print("  Search: 'python data analysis'")
conn.sql(
    """
    SELECT
        id,
        title,
        fts_main_docs.match_bm25(id, 'python data analysis') AS score
    FROM docs
    WHERE score IS NOT NULL
    ORDER BY score
    """
).show()
# Documents about Python AND data/analysis rank highest because they match
# more of the query terms.

print("  Search: 'SQL query performance'")
conn.sql(
    """
    SELECT
        id,
        title,
        fts_main_docs.match_bm25(id, 'SQL query performance') AS score
    FROM docs
    WHERE score IS NOT NULL
    ORDER BY score
    """
).show()

# =============================================================================
# 6. Search specific columns — index only title or only body
# =============================================================================
# You can create separate indexes on individual columns.  Let's create a
# title-only index to show how column-specific searching works.
print("=== 6. Search Specific Columns ===")

# Create a copy table so we can build a separate title-only index.
conn.execute("CREATE TABLE docs_title AS SELECT * FROM docs;")

conn.execute(
    """
    PRAGMA create_fts_index(
        'docs_title',
        'id',
        'title'           -- index only the title column
    );
    """
)

# Search the title-only index — only matches in the title column count.
print("  Title-only index — search for 'python':")
conn.sql(
    """
    SELECT
        id,
        title,
        fts_main_docs_title.match_bm25(id, 'python') AS score
    FROM docs_title
    WHERE score IS NOT NULL
    ORDER BY score
    """
).show()
# Fewer results than the full index because body-only mentions are excluded.

# Compare with the full (title + body) index:
print("  Full index (title + body) — search for 'python':")
conn.sql(
    """
    SELECT
        id,
        title,
        fts_main_docs.match_bm25(id, 'python') AS score
    FROM docs
    WHERE score IS NOT NULL
    ORDER BY score
    """
).show()
# The full index picks up documents where 'python' appears only in the body.

# =============================================================================
# 7. Practical: combine FTS with regular SQL filters
# =============================================================================
# FTS and standard SQL work together.  Use match_bm25() for relevance ranking
# and WHERE clauses for structured filtering (category, date ranges, etc.).
print("=== 7. Combine FTS with SQL Filters ===")

# Find documents about "query" but only in the 'database' category.
print("  Search 'query' filtered to category = 'database':")
conn.sql(
    """
    SELECT
        id,
        title,
        category,
        fts_main_docs.match_bm25(id, 'query') AS score
    FROM docs
    WHERE score IS NOT NULL
      AND category = 'database'
    ORDER BY score
    """
).show()

# Find documents about "analytics" but exclude the 'analytics' category
# to discover cross-topic mentions.
print("  Search 'analytics' excluding category = 'analytics':")
conn.sql(
    """
    SELECT
        id,
        title,
        category,
        fts_main_docs.match_bm25(id, 'analytics') AS score
    FROM docs
    WHERE score IS NOT NULL
      AND category != 'analytics'
    ORDER BY score
    """
).show()

# Combine FTS with LIMIT to get only the top-N most relevant results.
print("  Top 3 results for 'data':")
conn.sql(
    """
    SELECT
        id,
        title,
        fts_main_docs.match_bm25(id, 'data') AS score
    FROM docs
    WHERE score IS NOT NULL
    ORDER BY score
    LIMIT 3
    """
).show()

# =============================================================================
# 8. Recreating the index after data changes
# =============================================================================
# IMPORTANT: The FTS index is NOT automatically updated when you INSERT,
# UPDATE, or DELETE rows.  You must drop and recreate the index to reflect
# changes.  This is a key limitation to understand.
print("=== 8. Re-Index After Data Changes ===")

# Add two new documents to the table.
conn.execute(
    """
    INSERT INTO docs VALUES
    (11,
     'Machine Learning with Python and SQL',
     'Machine learning pipelines often start with SQL queries to extract training '
     'data from analytical databases. Python libraries like scikit-learn and '
     'XGBoost then build predictive models from the query results.',
     'python'),

    (12,
     'DuckDB Extensions Ecosystem',
     'DuckDB supports a rich ecosystem of extensions including fts for full-text '
     'search, httpfs for remote file access, and spatial for geospatial queries. '
     'Extensions are installed with INSTALL and activated with LOAD.',
     'database');
    """
)

# The old index does NOT include the new documents:
print("  Before re-index — search 'machine learning':")
conn.sql(
    """
    SELECT
        id,
        title,
        fts_main_docs.match_bm25(id, 'machine learning') AS score
    FROM docs
    WHERE score IS NOT NULL
    ORDER BY score
    """
).show()
print("  (No results — the new doc is not in the stale index.)\n")

# Drop the old index and recreate it to include the new data.
conn.execute("PRAGMA drop_fts_index('docs');")
print("  Old index dropped.")

conn.execute(
    """
    PRAGMA create_fts_index(
        'docs',
        'id',
        'title',
        'body'
    );
    """
)
print("  New index created.\n")

# Now the search finds the new documents:
print("  After re-index — search 'machine learning':")
conn.sql(
    """
    SELECT
        id,
        title,
        fts_main_docs.match_bm25(id, 'machine learning') AS score
    FROM docs
    WHERE score IS NOT NULL
    ORDER BY score
    """
).show()

# Also verify the other new document is indexed:
print("  After re-index — search 'extensions ecosystem':")
conn.sql(
    """
    SELECT
        id,
        title,
        fts_main_docs.match_bm25(id, 'extensions ecosystem') AS score
    FROM docs
    WHERE score IS NOT NULL
    ORDER BY score
    """
).show()

# -- Summary -------------------------------------------------------------------
print("=== Summary ===")
print(
    """
Key takeaways:
  - INSTALL fts; LOAD fts;          -- load the extension
  - PRAGMA create_fts_index(table, id_col, col1, col2, ...)
                                     -- build an inverted index
  - fts_main_<table>.match_bm25(id, 'search terms')
                                     -- returns BM25 relevance score (lower = better)
  - NULL score means no match        -- filter with WHERE score IS NOT NULL
  - Multi-word queries match any term, ranking docs with more matches higher
  - Index specific columns for targeted search (title-only, body-only)
  - Combine FTS with WHERE, ORDER BY, LIMIT for real-world search patterns
  - FTS index is NOT auto-updated    -- PRAGMA drop_fts_index + recreate after changes
"""
)

conn.close()
