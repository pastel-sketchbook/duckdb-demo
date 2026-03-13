"""Seaborn plots — statistical visualizations powered by DuckDB queries.

Demonstrates the DuckDB → pandas → seaborn workflow: let DuckDB handle the
heavy SQL (aggregation, pivoting, window functions), convert to a DataFrame
with .df(), and hand it to seaborn for polished statistical charts.

Charts use a warm, paper-tinted aesthetic: creamy gradient backgrounds with
pastel data colors — consistent with the matplotlib/plotly examples.

Charts are saved to examples/output/ — no interactive window is opened.

Covered plot types:
  - sns.barplot()   — average revenue by product (with error bars)
  - sns.heatmap()   — customer × product revenue matrix (DuckDB PIVOT)
  - sns.boxplot()   — distribution of order values by product
  - sns.stripplot() — order frequency by customer
  - sns.catplot()   — multi-faceted view across products and customers

Run with:
    uv run python examples/73_seaborn_plots.py
"""

from __future__ import annotations

from pathlib import Path

# Use the non-interactive Agg backend BEFORE importing pyplot.
# This avoids needing a display server and is required for headless
# environments (CI, containers, SSH sessions).
import matplotlib

matplotlib.use("Agg")

import duckdb
import matplotlib.pyplot as plt
import seaborn as sns

# -- Setup ---------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
CSV_FILE = DATA_DIR / "sales.csv"

# Output directory for saved charts
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Connect to DuckDB in-memory and load sales data
conn = duckdb.connect()
conn.execute(
    f"""
    CREATE TABLE sales AS
    SELECT
        *,
        quantity * price AS revenue   -- pre-compute revenue for convenience
    FROM read_csv('{CSV_FILE}', auto_detect = true)
    """
)

# ---------------------------------------------------------------------------
# Warm / creamy / paper-tinted style constants (matches 71_ and 72_)
# ---------------------------------------------------------------------------
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

PAPER_BG = "#fdf6ec"  # warm cream (figure)
AXES_BG = "#fefaf2"  # slightly lighter (axes)
TEXT_COLOR = "#5c4b3a"  # warm dark brown
SPINE_COLOR = "#d4c4a8"  # subtle warm spine

# Set the seaborn theme with our custom paper palette.
# "white" base style keeps things clean; we apply our own face/text colors.
sns.set_theme(
    style="white",
    palette=PASTEL_COLORS,
    font_scale=1.1,
    rc={
        "figure.facecolor": PAPER_BG,
        "axes.facecolor": AXES_BG,
        "text.color": TEXT_COLOR,
        "axes.labelcolor": TEXT_COLOR,
        "xtick.color": TEXT_COLOR,
        "ytick.color": TEXT_COLOR,
        "axes.edgecolor": SPINE_COLOR,
        "grid.color": SPINE_COLOR,
        "grid.alpha": 0.35,
        "grid.linewidth": 0.7,
    },
)


def save_fig(fig: plt.Figure, path: Path) -> None:
    """Save with the paper background and close."""
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)


print("Sales data loaded:")
conn.sql("SELECT * FROM sales ORDER BY date").show()

# =============================================================================
# 1. Bar Plot — average revenue by product (with error bars)
# =============================================================================
# DuckDB computes per-order revenue; seaborn calculates the mean and
# confidence interval automatically from the raw rows.
print("=== 1. Bar Plot: Average Revenue by Product ===")

# Query: one row per order with its revenue — seaborn will aggregate
bar_df = conn.sql(
    """
    SELECT product, revenue
    FROM sales
    ORDER BY product
    """
).df()

print(bar_df.to_string(index=False))
print()

fig, ax = plt.subplots(figsize=(7, 5))
sns.barplot(
    data=bar_df,
    x="product",
    y="revenue",
    estimator="mean",  # bar height = mean revenue per product
    errorbar="sd",  # error bars show standard deviation
    capsize=0.15,  # small caps on error bars for readability
    edgecolor=SPINE_COLOR,
    linewidth=0.8,
    ax=ax,
)
ax.set_title("Average Revenue per Order by Product", fontsize=14, fontweight="bold")
ax.set_xlabel("Product")
ax.set_ylabel("Revenue ($)")

bar_path = OUTPUT_DIR / "73_barplot_avg_revenue.png"
save_fig(fig, bar_path)
print(f"  Saved: {bar_path}")

# =============================================================================
# 2. Heatmap — customer × product revenue matrix (DuckDB PIVOT)
# =============================================================================
# This is where DuckDB really shines: PIVOT reshapes the data server-side,
# producing a matrix that maps directly to a heatmap.
print("\n=== 2. Heatmap: Customer × Product Revenue Matrix ===")

# Use DuckDB PIVOT to create a wide-format customer × product matrix.
# Each cell is the total revenue for that (customer, product) pair.
heat_df = conn.sql(
    """
    PIVOT (
        SELECT customer, product, SUM(revenue) AS revenue
        FROM sales
        GROUP BY customer, product
    )
    ON product
    USING SUM(revenue)
    GROUP BY customer
    ORDER BY customer
    """
).df()

# Set 'customer' as the index so seaborn uses it for the y-axis labels
heat_df = heat_df.set_index("customer")

print(heat_df.to_string())
print()

fig, ax = plt.subplots(figsize=(7, 5))
# Custom warm color map: cream → peach → deeper amber
warm_cmap = sns.blend_palette([PAPER_BG, "#e8a87c", "#c67a4b"], as_cmap=True)
sns.heatmap(
    heat_df,
    annot=True,  # print the dollar value inside each cell
    fmt=".2f",  # two decimal places
    cmap=warm_cmap,
    linewidths=0.5,  # thin lines between cells
    linecolor=SPINE_COLOR,
    ax=ax,
)
ax.set_title("Total Revenue: Customer × Product", fontsize=14, fontweight="bold")
ax.set_ylabel("Customer")
ax.set_xlabel("Product")

heat_path = OUTPUT_DIR / "73_heatmap_customer_product.png"
save_fig(fig, heat_path)
print(f"  Saved: {heat_path}")

# =============================================================================
# 3. Box Plot — distribution of order values by product
# =============================================================================
# Box plots reveal the spread, median, and potential outliers in each
# product's order values.  DuckDB supplies per-order rows; seaborn does
# the statistical summary.
print("\n=== 3. Box Plot: Order Value Distribution by Product ===")

# Query: every individual order with its revenue
box_df = conn.sql(
    """
    SELECT
        product,
        revenue,
        customer
    FROM sales
    ORDER BY product, revenue
    """
).df()

print(box_df.to_string(index=False))
print()

fig, ax = plt.subplots(figsize=(7, 5))
sns.boxplot(
    data=box_df,
    x="product",
    y="revenue",
    width=0.5,
    linewidth=1.2,
    boxprops=dict(edgecolor=TEXT_COLOR),
    whiskerprops=dict(color=TEXT_COLOR),
    capprops=dict(color=TEXT_COLOR),
    medianprops=dict(color="#c67a4b", linewidth=2),
    ax=ax,
)
# Overlay the individual data points so we can see every order
sns.stripplot(
    data=box_df,
    x="product",
    y="revenue",
    color=TEXT_COLOR,  # warm dark dots
    size=6,
    alpha=0.6,
    jitter=True,
    ax=ax,
)
ax.set_title("Order Value Distribution by Product", fontsize=14, fontweight="bold")
ax.set_xlabel("Product")
ax.set_ylabel("Revenue ($)")

box_path = OUTPUT_DIR / "73_boxplot_order_values.png"
save_fig(fig, box_path)
print(f"  Saved: {box_path}")

# =============================================================================
# 4. Strip Plot — order frequency and size by customer
# =============================================================================
# A strip plot shows every individual observation.  Here, each dot is one
# order; the x-axis groups by customer and the y-axis shows revenue.
# This reveals both how many orders each customer placed and how large
# they were.
print("\n=== 4. Strip Plot: Orders by Customer ===")

# DuckDB adds an order rank per customer so we can annotate if needed
strip_df = conn.sql(
    """
    SELECT
        customer,
        date,
        product,
        revenue,
        ROW_NUMBER() OVER (
            PARTITION BY customer ORDER BY date
        ) AS order_num
    FROM sales
    ORDER BY customer, date
    """
).df()

print(strip_df.to_string(index=False))
print()

fig, ax = plt.subplots(figsize=(7, 5))
sns.stripplot(
    data=strip_df,
    x="customer",
    y="revenue",
    hue="product",  # color-code by product
    size=9,
    jitter=0.2,  # slight horizontal jitter to avoid overlap
    dodge=True,  # separate hue groups side by side
    edgecolor=SPINE_COLOR,
    linewidth=0.5,
    ax=ax,
)
ax.set_title("Individual Orders by Customer (colored by product)", fontsize=14, fontweight="bold")
ax.set_xlabel("Customer")
ax.set_ylabel("Revenue ($)")
ax.legend(title="Product", bbox_to_anchor=(1.02, 1), loc="upper left")

strip_path = OUTPUT_DIR / "73_stripplot_customer_orders.png"
save_fig(fig, strip_path)
print(f"  Saved: {strip_path}")

# =============================================================================
# 5. Catplot — multi-faceted view: revenue by product, faceted by quarter
# =============================================================================
# sns.catplot() creates a FacetGrid of categorical plots.  We use DuckDB
# to compute the quarter label, then let seaborn lay out one panel per
# quarter.  This gives a compact view of how each product performed over
# time.
print("\n=== 5. Catplot: Revenue by Product, Faceted by Quarter ===")

# DuckDB extracts the quarter and builds a label like "Q1 2024"
cat_df = conn.sql(
    """
    SELECT
        product,
        customer,
        revenue,
        'Q' || QUARTER(date) || ' ' || YEAR(date) AS quarter
    FROM sales
    ORDER BY date
    """
).df()

print(cat_df.to_string(index=False))
print()

# catplot returns its own Figure, not an Axes — so we use the g object
g = sns.catplot(
    data=cat_df,
    x="product",
    y="revenue",
    col="quarter",  # one sub-plot per quarter
    kind="bar",  # bar chart in each facet
    estimator="sum",  # total revenue (not mean) per product per quarter
    errorbar=None,  # no error bars — we're showing totals
    height=4,
    aspect=0.9,
    col_wrap=3,  # max 3 columns before wrapping
    edgecolor=SPINE_COLOR,
    linewidth=0.8,
)
g.set_titles("Quarter: {col_name}")
g.set_axis_labels("Product", "Revenue ($)")
g.figure.suptitle(
    "Total Revenue by Product per Quarter",
    y=1.03,
    fontsize=14,
    fontweight="bold",
    color=TEXT_COLOR,
)
# Apply paper background to the catplot figure
g.figure.patch.set_facecolor(PAPER_BG)
for ax in g.axes.flat:
    ax.set_facecolor(AXES_BG)

cat_path = OUTPUT_DIR / "73_catplot_quarterly.png"
g.savefig(cat_path, dpi=150, bbox_inches="tight", facecolor=g.figure.get_facecolor())
plt.close(g.figure)
print(f"  Saved: {cat_path}")

# =============================================================================
# 6. Summary
# =============================================================================
print("\n=== Summary ===")
print(
    f"""
All charts saved to: {OUTPUT_DIR}/

Files created:
  73_barplot_avg_revenue.png      — mean revenue per product (with SD)
  73_heatmap_customer_product.png — customer × product revenue matrix
  73_boxplot_order_values.png     — order value distributions + strip overlay
  73_stripplot_customer_orders.png— individual orders by customer & product
  73_catplot_quarterly.png        — quarterly revenue faceted by product

Key takeaways:
  - DuckDB handles aggregation, PIVOT, and window functions in SQL
  - .df() converts DuckDB results to a pandas DataFrame for seaborn
  - seaborn's statistical estimators (mean, sum, CI) work on raw rows
  - sns.catplot() with col= creates faceted grids for time-based analysis
  - matplotlib.use("Agg") + plt.savefig() = no GUI window needed
"""
)

conn.close()
