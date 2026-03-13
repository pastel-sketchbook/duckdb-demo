"""Interactive charts with Plotly Express — let DuckDB do the heavy lifting.

DuckDB handles all SQL aggregation, then .df() converts results to pandas
DataFrames that Plotly Express can chart directly.  Each chart is saved as
a self-contained interactive HTML file (no browser popup).

Charts use a warm, paper-tinted aesthetic: creamy backgrounds with pastel
data colors — consistent with the matplotlib/seaborn examples in this track.

Run with:
    uv run python examples/72_plotly_charts.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import plotly.express as px
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# Setup — paths and connection
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

# All charts are written here — the directory is created if missing.
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

conn = duckdb.connect()

# Load sales.csv into a DuckDB table so every section can query it.
conn.execute(
    f"""
    CREATE TABLE sales AS
    SELECT * FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)

# ---------------------------------------------------------------------------
# Warm / creamy / paper-tinted style constants (matches 71_ and 73_)
# ---------------------------------------------------------------------------
# Pastel color sequence for data elements.
PASTEL_COLORS = [
    "#e8a87c",  # warm peach
    "#85cdca",  # soft teal
    "#d5a6bd",  # dusty rose
    "#b5c99a",  # sage green
    "#f0c987",  # golden sand
    "#9db4c0",  # pale slate blue
    "#c9b1d0",  # lavender
    "#f4b9a0",  # salmon cream
]

PAPER_BG = "#fdf6ec"  # warm cream (outer)
PLOT_BG = "#fefaf2"  # slightly lighter (inner plot)
GRID_COLOR = "#e6dcc8"  # subtle warm grid
TEXT_COLOR = "#5c4b3a"  # warm dark brown


def apply_paper_layout(fig: go.Figure) -> None:
    """Apply the warm paper-tinted theme to a Plotly figure."""
    fig.update_layout(
        paper_bgcolor=PAPER_BG,
        plot_bgcolor=PLOT_BG,
        font=dict(color=TEXT_COLOR, size=13),
        title_font=dict(color=TEXT_COLOR, size=16),
        xaxis=dict(gridcolor=GRID_COLOR, linecolor=GRID_COLOR),
        yaxis=dict(gridcolor=GRID_COLOR, linecolor=GRID_COLOR),
        legend=dict(bgcolor="rgba(253,246,236,0.7)"),
    )


# =========================================================================
# 1. Setup verification — peek at the raw data
# =========================================================================
print("=== Setup: sales data loaded ===")
conn.sql("SELECT * FROM sales LIMIT 5").show()

# =========================================================================
# 2. Bar chart — total revenue by product
# =========================================================================
# DuckDB computes the aggregate; Plotly just renders it.
print("\n=== Bar Chart: Revenue by Product ===")

df_product_rev = conn.sql(
    """
    SELECT
        product,
        ROUND(SUM(quantity * price), 2) AS revenue
    FROM sales
    GROUP BY product
    ORDER BY revenue DESC
    """
).df()  # .df() → pandas DataFrame

fig = px.bar(
    df_product_rev,
    x="product",
    y="revenue",
    title="Total Revenue by Product",
    labels={"product": "Product", "revenue": "Revenue ($)"},
    color="product",  # one color per bar
    color_discrete_sequence=PASTEL_COLORS,
    text_auto=".2f",  # show values on bars
)
fig.update_layout(showlegend=False)  # legend is redundant here
apply_paper_layout(fig)

bar_path = OUTPUT_DIR / "bar_revenue_by_product.html"
fig.write_html(str(bar_path))
print(f"  Saved → {bar_path}")

# =========================================================================
# 3. Line chart — monthly revenue trend with markers
# =========================================================================
# DATE_TRUNC rolls every date up to the first of its month.
print("\n=== Line Chart: Monthly Revenue Trend ===")

df_monthly = conn.sql(
    """
    SELECT
        DATE_TRUNC('month', date)::DATE   AS month,
        ROUND(SUM(quantity * price), 2)   AS revenue
    FROM sales
    GROUP BY month
    ORDER BY month
    """
).df()

fig = px.line(
    df_monthly,
    x="month",
    y="revenue",
    title="Monthly Revenue Trend",
    labels={"month": "Month", "revenue": "Revenue ($)"},
    markers=True,  # dots at each data point
    color_discrete_sequence=[PASTEL_COLORS[1]],  # soft teal
)
fig.update_traces(line=dict(width=3))  # thicker line for clarity
apply_paper_layout(fig)

line_path = OUTPUT_DIR / "line_monthly_revenue.html"
fig.write_html(str(line_path))
print(f"  Saved → {line_path}")

# =========================================================================
# 4. Scatter plot — quantity vs revenue, colored by product
# =========================================================================
# Here we keep row-level detail (no aggregation) so each order is a dot.
print("\n=== Scatter Plot: Quantity vs Revenue by Product ===")

df_scatter = conn.sql(
    """
    SELECT
        product,
        customer,
        quantity,
        ROUND(quantity * price, 2) AS revenue
    FROM sales
    """
).df()

fig = px.scatter(
    df_scatter,
    x="quantity",
    y="revenue",
    color="product",  # color-code by product
    color_discrete_sequence=PASTEL_COLORS,
    hover_data=["customer"],  # show customer on hover
    title="Order Quantity vs Revenue (colored by Product)",
    labels={
        "quantity": "Quantity (units)",
        "revenue": "Revenue ($)",
        "product": "Product",
    },
    size="quantity",  # bubble size = quantity
    size_max=20,
)
apply_paper_layout(fig)

scatter_path = OUTPUT_DIR / "scatter_qty_vs_revenue.html"
fig.write_html(str(scatter_path))
print(f"  Saved → {scatter_path}")

# =========================================================================
# 5. Stacked bar — monthly revenue stacked by product
# =========================================================================
# Two GROUP BY columns (month + product) give us the breakdown Plotly needs
# for stacked bars.
print("\n=== Stacked Bar: Monthly Revenue by Product ===")

df_stacked = conn.sql(
    """
    SELECT
        DATE_TRUNC('month', date)::DATE   AS month,
        product,
        ROUND(SUM(quantity * price), 2)   AS revenue
    FROM sales
    GROUP BY month, product
    ORDER BY month, product
    """
).df()

fig = px.bar(
    df_stacked,
    x="month",
    y="revenue",
    color="product",  # each product gets its own slice
    color_discrete_sequence=PASTEL_COLORS,
    barmode="stack",  # stack slices on top of each other
    title="Monthly Revenue Stacked by Product",
    labels={
        "month": "Month",
        "revenue": "Revenue ($)",
        "product": "Product",
    },
    text_auto=".0f",  # label each slice
)
apply_paper_layout(fig)

stacked_path = OUTPUT_DIR / "stacked_bar_monthly.html"
fig.write_html(str(stacked_path))
print(f"  Saved → {stacked_path}")

# =========================================================================
# 6. Sunburst — hierarchical view: customer → product → revenue
# =========================================================================
# A sunburst chart shows part-to-whole relationships across multiple
# hierarchy levels.  The inner ring is customer, the outer ring is product.
print("\n=== Sunburst: Customer → Product → Revenue ===")

df_sunburst = conn.sql(
    """
    SELECT
        customer,
        product,
        ROUND(SUM(quantity * price), 2) AS revenue
    FROM sales
    GROUP BY customer, product
    ORDER BY customer, product
    """
).df()

fig = px.sunburst(
    df_sunburst,
    path=["customer", "product"],  # hierarchy levels (inner → outer)
    values="revenue",  # size of each slice
    title="Revenue Breakdown: Customer → Product",
    color="revenue",  # color intensity = revenue
    color_continuous_scale=[
        [0, "#fdf6ec"],  # warm cream (low)
        [0.5, "#e8a87c"],  # warm peach (mid)
        [1, "#c67a4b"],  # deeper amber (high)
    ],
)
fig.update_traces(textinfo="label+percent parent")  # show name + % of parent
apply_paper_layout(fig)

sunburst_path = OUTPUT_DIR / "sunburst_customer_product.html"
fig.write_html(str(sunburst_path))
print(f"  Saved → {sunburst_path}")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n=== All charts saved to examples/output/ ===")
print(f"  1. {bar_path.name}")
print(f"  2. {line_path.name}")
print(f"  3. {scatter_path.name}")
print(f"  4. {stacked_path.name}")
print(f"  5. {sunburst_path.name}")
print("\nOpen any .html file in a browser for full interactivity (zoom, hover, pan).")

conn.close()
