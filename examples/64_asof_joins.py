"""ASOF Joins — time-series alignment with nearest-match joins.

ASOF joins match each row from the left table with the *closest* row from the
right table based on an inequality condition (typically timestamps).  Unlike a
regular join that requires an exact match, ASOF finds the nearest qualifying
row — making it indispensable for time-series analytics.

Common use cases:
  - Match trades to the most recent quote (finance)
  - Attribute user events to the most recent marketing campaign
  - Align sensor readings that arrive on slightly different clocks

Run with:
    uv run python examples/64_asof_joins.py
"""

from __future__ import annotations

import duckdb

conn = duckdb.connect()

# =============================================================================
# 1. Setup — stock trades and stock quotes with different timestamps
# =============================================================================
# Trades happen at exact moments.  Quotes (bid/ask prices) are published at
# slightly different times.  There is almost never an exact timestamp match
# between the two — which is exactly why ASOF joins exist.
print("=== 1. Setup: trades and quotes tables ===")

conn.execute(
    """
    CREATE TABLE trades (
        symbol  VARCHAR,
        ts      TIMESTAMP,
        qty     INTEGER,
        price   DOUBLE
    );

    INSERT INTO trades VALUES
        ('AAPL', '2024-01-15 10:00:01', 100, 185.50),
        ('AAPL', '2024-01-15 10:00:04', 200, 185.75),
        ('AAPL', '2024-01-15 10:00:07', 150, 186.00),
        ('GOOG', '2024-01-15 10:00:02', 50,  141.20),
        ('GOOG', '2024-01-15 10:00:06', 75,  141.50);
    """
)

conn.execute(
    """
    CREATE TABLE quotes (
        symbol  VARCHAR,
        ts      TIMESTAMP,
        bid     DOUBLE,
        ask     DOUBLE
    );

    INSERT INTO quotes VALUES
        ('AAPL', '2024-01-15 09:59:58', 185.40, 185.60),
        ('AAPL', '2024-01-15 10:00:02', 185.55, 185.70),
        ('AAPL', '2024-01-15 10:00:05', 185.70, 185.85),
        ('GOOG', '2024-01-15 09:59:59', 141.00, 141.30),
        ('GOOG', '2024-01-15 10:00:03', 141.15, 141.40),
        ('GOOG', '2024-01-15 10:00:08', 141.45, 141.65);
    """
)

print("Trades:")
conn.sql("SELECT * FROM trades ORDER BY ts").show()

print("Quotes:")
conn.sql("SELECT * FROM quotes ORDER BY ts").show()

# =============================================================================
# 2. Basic ASOF JOIN — most recent quote at or before each trade
# =============================================================================
# Syntax:  FROM left ASOF JOIN right ON left.key = right.key AND left.ts >= right.ts
#
# For each trade, DuckDB finds the quote with the LARGEST ts that is still
# <= the trade's ts (within the same symbol).  This is the "look backward"
# pattern — the most common use of ASOF joins.
print("=== 2. Basic ASOF JOIN: most recent quote at or before each trade ===")

conn.sql(
    """
    SELECT
        t.symbol,
        t.ts         AS trade_time,
        t.price      AS trade_price,
        q.ts         AS quote_time,
        q.bid,
        q.ask,
        -- The spread tells us how much the market-maker was charging
        ROUND(q.ask - q.bid, 2) AS spread
    FROM trades t
    ASOF JOIN quotes q
        ON  t.symbol = q.symbol     -- exact match on symbol
        AND t.ts     >= q.ts        -- nearest quote at-or-before the trade
    ORDER BY t.ts
    """
).show()
# Notice:
#   - AAPL trade at 10:00:01 matches quote at 09:59:58 (the only quote before it)
#   - AAPL trade at 10:00:04 matches quote at 10:00:02 (skipping the 09:59:58 one)
#   - GOOG trade at 10:00:02 matches quote at 09:59:59

# =============================================================================
# 3. Compare ASOF JOIN vs regular JOIN — why exact match fails
# =============================================================================
# A regular (INNER) JOIN on timestamp requires an exact match.  In real
# time-series data, trades and quotes almost never share the exact same
# timestamp, so a regular join drops most rows.
print("=== 3. Regular JOIN on timestamp: mostly empty! ===")

conn.sql(
    """
    SELECT
        t.symbol,
        t.ts   AS trade_time,
        q.ts   AS quote_time,
        t.price,
        q.bid,
        q.ask
    FROM trades t
    INNER JOIN quotes q
        ON  t.symbol = q.symbol
        AND t.ts     = q.ts       -- exact match — rarely works!
    ORDER BY t.ts
    """
).show()
# Expected: zero or very few rows, because the timestamps don't align exactly.

print("(Regular JOIN returned few/no rows — that's the problem ASOF solves.)\n")

# =============================================================================
# 4. ASOF JOIN with >= on the right — forward-looking (next quote AFTER trade)
# =============================================================================
# Flip the inequality so DuckDB finds the SMALLEST right-side ts that is
# >= the left-side ts.  This answers: "What was the first quote published
# AFTER each trade?"
print("=== 4. Forward-looking ASOF JOIN: next quote after each trade ===")

conn.sql(
    """
    SELECT
        t.symbol,
        t.ts         AS trade_time,
        t.price      AS trade_price,
        q.ts         AS next_quote_time,
        q.bid        AS next_bid,
        q.ask        AS next_ask
    FROM trades t
    ASOF JOIN quotes q
        ON  t.symbol = q.symbol
        AND t.ts     <= q.ts        -- nearest quote at-or-after the trade
    ORDER BY t.ts
    """
).show()
# Now each trade is matched to the NEXT quote that was published.
# - AAPL trade at 10:00:01 -> quote at 10:00:02
# - AAPL trade at 10:00:04 -> quote at 10:00:05

# =============================================================================
# 5. Practical: event attribution — match user events to campaigns
# =============================================================================
# A marketing team launches campaigns at various times.  Each user event
# should be attributed to the campaign that was most recently started.
print("=== 5. Practical: attribute user events to the most recent campaign ===")

conn.execute(
    """
    CREATE TABLE campaigns (
        campaign_id   VARCHAR,
        started_at    TIMESTAMP,
        channel       VARCHAR
    );

    INSERT INTO campaigns VALUES
        ('C1', '2024-03-01 08:00:00', 'email'),
        ('C2', '2024-03-05 12:00:00', 'social'),
        ('C3', '2024-03-10 09:00:00', 'search');
    """
)

conn.execute(
    """
    CREATE TABLE user_events (
        user_id    INTEGER,
        event_ts   TIMESTAMP,
        action     VARCHAR
    );

    INSERT INTO user_events VALUES
        (1, '2024-03-02 14:30:00', 'page_view'),
        (2, '2024-03-05 12:05:00', 'signup'),
        (3, '2024-03-07 09:00:00', 'purchase'),
        (4, '2024-03-10 10:00:00', 'signup'),
        (5, '2024-03-15 16:00:00', 'purchase');
    """
)

# Each event is attributed to the campaign that was active most recently.
# No equality key here — we match purely on the timestamp inequality.
conn.sql(
    """
    SELECT
        e.user_id,
        e.event_ts,
        e.action,
        c.campaign_id,
        c.channel       AS attributed_channel,
        c.started_at    AS campaign_start
    FROM user_events e
    ASOF JOIN campaigns c
        ON e.event_ts >= c.started_at
    ORDER BY e.event_ts
    """
).show()
# - User 1 (Mar 2) -> C1 (email, started Mar 1)
# - User 2 (Mar 5 12:05) -> C2 (social, started Mar 5 12:00 — just 5 min ago)
# - User 3 (Mar 7) -> C2 (social, still the most recent campaign)
# - User 4 (Mar 10 10:00) -> C3 (search, started Mar 10 09:00)
# - User 5 (Mar 15) -> C3 (search, still the most recent)

# =============================================================================
# 6. Multiple ASOF joins — enrich trades with both bid and ask from separate tables
# =============================================================================
# Sometimes bid and ask quotes arrive in separate feeds.  You can chain
# multiple ASOF joins to pull in data from different sources.
print("=== 6. Multiple ASOF joins: bid and ask from separate feeds ===")

conn.execute(
    """
    CREATE TABLE bid_quotes (
        symbol  VARCHAR,
        ts      TIMESTAMP,
        bid     DOUBLE
    );

    INSERT INTO bid_quotes VALUES
        ('AAPL', '2024-01-15 09:59:57', 185.38),
        ('AAPL', '2024-01-15 10:00:02', 185.55),
        ('AAPL', '2024-01-15 10:00:06', 185.72);
    """
)

conn.execute(
    """
    CREATE TABLE ask_quotes (
        symbol  VARCHAR,
        ts      TIMESTAMP,
        ask     DOUBLE
    );

    INSERT INTO ask_quotes VALUES
        ('AAPL', '2024-01-15 09:59:59', 185.62),
        ('AAPL', '2024-01-15 10:00:03', 185.71),
        ('AAPL', '2024-01-15 10:00:05', 185.84);
    """
)

# Chain two ASOF joins — one for the latest bid, one for the latest ask.
conn.sql(
    """
    SELECT
        t.ts              AS trade_time,
        t.price           AS trade_price,
        b.ts              AS bid_time,
        b.bid,
        a.ts              AS ask_time,
        a.ask,
        ROUND(a.ask - b.bid, 2) AS implied_spread
    FROM trades t
    ASOF JOIN bid_quotes b
        ON  t.symbol = b.symbol
        AND t.ts     >= b.ts
    ASOF JOIN ask_quotes a
        ON  t.symbol = a.symbol
        AND t.ts     >= a.ts
    WHERE t.symbol = 'AAPL'
    ORDER BY t.ts
    """
).show()
# Each trade now carries the most recent bid AND the most recent ask,
# even though those quotes arrived at different times.

# =============================================================================
# 7. Key patterns: when to use ASOF joins vs window functions
# =============================================================================
print("=== 7. Key patterns & when to use ASOF joins ===")
print(
    """
ASOF JOIN syntax:
  FROM left_table
  ASOF JOIN right_table
      ON  left.key   = right.key      -- exact-match columns (optional)
      AND left.ts   >= right.ts       -- inequality on the ordering column

  The inequality condition MUST be the last ON clause.  DuckDB supports
  >=  (look backward — most recent right row at-or-before left row)
  <=  (look forward  — next right row at-or-after left row)
  >   (strictly before) and <  (strictly after)

When to use ASOF JOIN:
  1. Aligning two time-series with different cadences (trades & quotes)
  2. Event attribution (match events to the latest campaign/session/status)
  3. Snapshot lookups (find the effective price/rate/config at a point in time)
  4. Sensor fusion (combine readings from sensors on different clocks)

ASOF JOIN vs window functions:
  - Use ASOF JOIN when you have TWO separate tables to align.
  - Use window functions (LAG, LEAD, LAST_VALUE) when you need to look at
    neighboring rows WITHIN the same table.
  - ASOF JOIN is typically faster and cleaner than a correlated subquery
    that tries to find "the most recent row where ts <= my_ts".

Performance notes:
  - DuckDB sorts internally, but pre-sorted data helps.
  - The exact-match columns (before AND) act as a partition key, so ASOF
    matching only happens within each partition — very efficient.
"""
)

conn.close()
