# Track 3 — Intermediate SQL: Rationale

## Intention

Track 3 elevates the learner from "I can write queries" to "I can write
*analytical* queries." The six examples cover the features that separate casual
SQL users from effective data analysts: window functions, temporal logic, text
processing, data reshaping, and DuckDB's unique syntax shortcuts.

Every example is framed around a real question ("Who was the top customer each
month?", "How do I normalize phone numbers?", "How do I pivot this for a
report?") so the learner understands *when* to reach for each tool.

## Why these six examples, in this order

### 31 — Window functions: ranking

Window functions are the single biggest unlock for analytical SQL. Ranking
functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE) are the most intuitive entry
point because they answer questions learners already understand:

- "Who came first?" → ROW_NUMBER
- "What rank, allowing ties?" → RANK / DENSE_RANK
- "Which quartile?" → NTILE

Design choices:

- **Top-N per group** is shown as the #1 use case because it is by far the most
  common window function pattern in production analytics.
- **Side-by-side comparison** of ROW_NUMBER vs RANK vs DENSE_RANK uses the same
  data so the tie-handling difference is visually obvious.
- **NTILE for customer segmentation** (Gold/Silver/Bronze) connects the abstract
  function to a concrete business outcome.

### 32 — Window functions: aggregates

After ranking, aggregate windows are the next step. These compute values *across*
rows (running totals, moving averages) without collapsing the result:

- **Running total** — the simplest window aggregate. The default frame clause
  produces the cumulative sum, which is the most common time-series metric.
- **Moving average** — introduces explicit frame clauses (`ROWS BETWEEN`). The
  3-month centered window is a classic smoothing technique.
- **LAG / LEAD** — period-over-period comparison. Month-over-month change is
  the bread and butter of business reporting.
- **FIRST_VALUE / LAST_VALUE** — anchoring to window boundaries.
- **Frame clause comparison** — a single query showing how `UNBOUNDED PRECEDING`,
  `1 PRECEDING`, and `()` (full partition) produce different results from the
  same SUM().

The key pedagogical goal is building intuition for *what the frame clause does*.
Without understanding frames, window functions remain magical.

### 33 — Date & time functions

Dates are the most common dimension in analytics. This example covers:

- **DATE_TRUNC** — the essential grouping function. Shown first because learners
  already used it casually in earlier examples and now see it properly.
- **DATE_PART / EXTRACT** — pulling components out. The day-of-week extraction
  enables time-pattern analysis.
- **DATE_DIFF** — measuring distances. "Days since first order" is a natural
  cohort metric.
- **DATE_ADD / INTERVAL** — moving dates. The multiple syntax forms (operator
  and function) are both shown.
- **STRFTIME** — formatting for reports. Learners often struggle with format
  codes; showing side-by-side output makes them concrete.
- **generate_series for gap filling** — a practical pattern. Real sales data
  has missing months; generating a continuous date range and LEFT JOINing is
  the standard fix.

### 34 — String functions & regex

Text processing is unavoidable in real data. This example uses a `contacts`
table with messy names, emails, phone numbers, and addresses:

- **LIKE / ILIKE** — review from Track 2, now with richer examples.
- **CONCAT** — both `||` and `concat()` forms.
- **UPPER / LOWER** — case normalization.
- **STRING_SPLIT** — parsing structured text (city/state separation).
- **REGEXP_MATCHES / REGEXP_EXTRACT / REGEXP_REPLACE** — the power tools.
  Phone number normalization is the running example because it's messy,
  relatable, and shows regex groups clearly.
- **STRING_AGG** — the aggregation counterpart. Listing contacts per state
  is a common reporting need.

Using inline data (not sales.csv) is deliberate: string functions need diverse,
messy text that a clean sales dataset doesn't provide.

### 35 — PIVOT & UNPIVOT

Data reshaping is the bridge between analytical queries and reporting. This
example shows:

- **Long format → wide format (PIVOT)** — the primary use case. Monthly
  product revenue as a cross-tab is the canonical example.
- **Multiple aggregates in PIVOT** — SUM and COUNT simultaneously.
- **PIVOT with IN** — restricting which values become columns.
- **Wide → long (UNPIVOT)** — the inverse operation. Shown with both explicit
  column lists and `COLUMNS(* EXCLUDE ...)`.
- **Customer × product matrix** — a practical cross-tab that a stakeholder
  would actually ask for.

The "before and after" pattern (show long table, then pivoted table) follows
the SKILL.md guideline for PIVOT examples.

### 36 — Friendly SQL extensions

This is the "DuckDB appreciation" example — features that make DuckDB's SQL
dialect more ergonomic than standard SQL:

- **QUALIFY** — filter on window functions without a CTE. Shown side-by-side
  with the traditional CTE approach so the readability win is obvious.
- **SAMPLE** — random subsets for exploration. Both count and percentage forms.
- **EXCLUDE / REPLACE** — `SELECT *` modifiers that avoid listing every column.
- **COLUMNS()** — apply operations to multiple columns by pattern. This is
  uniquely powerful and has no equivalent in most databases.
- **GROUP BY ALL / ORDER BY ALL** — reduce boilerplate.
- **FROM-first syntax** — DuckDB's alternative query ordering. Shown last as
  a "one more thing" feature.

These features don't fit neatly into any single topic, so bundling them into a
"friendly SQL" example gives them a home while showcasing DuckDB's developer
experience advantages.

## Design decisions

- **Window functions get two full examples** (31 and 32) because they are the
  most conceptually challenging feature in this track. Splitting ranking from
  aggregates reduces cognitive load.
- **Inline data for string example** — sales.csv has clean, uniform text.
  Messy phone numbers and emails require purpose-built data.
- **DuckDB-specific features in example 36** are always shown *after* the
  standard equivalent, so learners understand both and can use the shortcut
  when DuckDB is available.
- **No ICU extension** — the dates example uses built-in functions only. ICU
  (for time zones and locale-aware formatting) is deferred to Track 6 where
  advanced features live.
