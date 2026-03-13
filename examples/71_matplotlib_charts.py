"""Matplotlib charts from DuckDB query results.

Demonstrates the core DuckDB-to-matplotlib workflow: run SQL aggregations in
DuckDB, convert to pandas with ``.df()``, then plot with matplotlib.  Every
chart is saved to ``examples/output/`` so the script works in headless (CI /
SSH) environments — no ``plt.show()`` calls.

Charts use a warm, paper-tinted aesthetic: creamy gradient backgrounds with
pastel data colors — designed to feel like a hand-drawn notebook.

Covers bar charts, line charts, horizontal bar charts, grouped bar charts,
and combined bar + line overlays.

Run with:
    uv run python examples/71_matplotlib_charts.py
"""

from __future__ import annotations

from pathlib import Path

# Use the non-interactive Agg backend BEFORE importing pyplot.
# This avoids "Tcl/Tk not found" errors in headless environments.
import matplotlib

matplotlib.use("Agg")

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.axes import Axes

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent / "src" / "duckdb_demo" / "data"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"

# Ensure the output directory exists (like `mkdir -p`)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Warm / creamy / paper-tinted style constants
# ---------------------------------------------------------------------------
# A soft pastel palette — warm and easy on the eyes.
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

# Gradient endpoints for the figure background (top → bottom).
PAPER_LIGHT = "#fdf6ec"  # warm cream (top)
PAPER_DARK = "#f5ead0"  # parchment tan (bottom)

# Axes face color — slightly lighter than the outer gradient.
AXES_BG = "#fefaf2"

# Subtle text and spine color.
TEXT_COLOR = "#5c4b3a"
SPINE_COLOR = "#d4c4a8"


def _style_ax(ax: Axes) -> None:
    """Apply paper-tinted styling to a single Axes."""
    ax.set_facecolor(AXES_BG)
    ax.title.set_color(TEXT_COLOR)
    ax.xaxis.label.set_color(TEXT_COLOR)
    ax.yaxis.label.set_color(TEXT_COLOR)
    ax.tick_params(colors=TEXT_COLOR, which="both")
    for spine in ax.spines.values():
        spine.set_color(SPINE_COLOR)
    # Soften the grid (if present)
    ax.grid(True, color=SPINE_COLOR, alpha=0.35, linewidth=0.7)


def apply_paper_style(fig: plt.Figure, *axes: Axes) -> None:
    """Apply the warm paper-tinted background to *fig* and each *axes*.

    Call this AFTER all plotting is done but BEFORE tight_layout / savefig
    so that background colours render correctly.
    """
    fig.patch.set_facecolor(PAPER_LIGHT)
    fig.patch.set_alpha(1)

    for ax in axes:
        _style_ax(ax)


def save_styled(fig: plt.Figure, path: Path) -> None:
    """Save *fig* with the paper background and close it."""
    fig.savefig(path, dpi=150, facecolor=fig.get_facecolor(), edgecolor="none")
    plt.close(fig)


# =============================================================================
# 1. Setup — load sales.csv into DuckDB
# =============================================================================
print("=== 1. Setup ===")

conn = duckdb.connect()  # in-memory database

# DuckDB can read CSV directly — no pandas needed for ingestion.
conn.execute(
    f"""
    CREATE TABLE sales AS
    SELECT * FROM read_csv('{DATA_DIR / "sales.csv"}', auto_detect = true)
    """
)

# Quick sanity check: show the raw data
print("Loaded sales data:")
conn.sql("SELECT * FROM sales ORDER BY date").show()
print(f"Output directory: {OUTPUT_DIR}\n")

# =============================================================================
# 2. Bar chart — revenue by product
# =============================================================================
# Strategy: DuckDB does the aggregation, pandas holds the result, matplotlib
# draws the chart.
print("=== 2. Bar Chart: Revenue by Product ===")

# Heavy lifting in SQL — compute revenue (quantity * price) per product
product_revenue = conn.sql(
    """
    SELECT
        product,
        ROUND(SUM(quantity * price), 2) AS revenue
    FROM sales
    GROUP BY product
    ORDER BY revenue DESC
    """
).df()  # .df() converts the DuckDB result to a pandas DataFrame

print(product_revenue.to_string(index=False))

# Create a simple vertical bar chart
fig, ax = plt.subplots(figsize=(7, 5))
ax.bar(
    product_revenue["product"],
    product_revenue["revenue"],
    color=PASTEL_COLORS[:3],
    edgecolor=SPINE_COLOR,
    linewidth=0.8,
)
ax.set_title("Total Revenue by Product", fontsize=14, fontweight="bold")
ax.set_xlabel("Product")
ax.set_ylabel("Revenue ($)")

# Add value labels on top of each bar
for i, val in enumerate(product_revenue["revenue"]):
    ax.text(i, val + 5, f"${val:,.2f}", ha="center", va="bottom", fontsize=10, color=TEXT_COLOR)

apply_paper_style(fig, ax)
fig.tight_layout()
out_path = OUTPUT_DIR / "bar_revenue_by_product.png"
save_styled(fig, out_path)
print(f"Saved: {out_path}\n")

# =============================================================================
# 3. Line chart — monthly revenue trend
# =============================================================================
# DuckDB's date_trunc() rolls each date up to the first of its month,
# giving us a clean monthly grouping in pure SQL.
print("=== 3. Line Chart: Monthly Revenue Trend ===")

monthly_trend = conn.sql(
    """
    SELECT
        date_trunc('month', date) AS month,
        ROUND(SUM(quantity * price), 2) AS revenue
    FROM sales
    GROUP BY month
    ORDER BY month
    """
).df()

# Format the month column for clean x-axis labels (e.g. "Jan 2024")
monthly_trend["label"] = monthly_trend["month"].dt.strftime("%b %Y")

print(monthly_trend[["label", "revenue"]].to_string(index=False))

fig, ax = plt.subplots(figsize=(8, 5))
ax.plot(
    monthly_trend["label"],
    monthly_trend["revenue"],
    marker="o",
    linewidth=2.5,
    color=PASTEL_COLORS[1],  # soft teal
    markeredgecolor=TEXT_COLOR,
    markeredgewidth=0.8,
    markersize=8,
)

# Annotate each data point with its value
for _, row in monthly_trend.iterrows():
    ax.annotate(
        f"${row['revenue']:,.2f}",
        xy=(row["label"], row["revenue"]),
        textcoords="offset points",
        xytext=(0, 12),
        ha="center",
        fontsize=9,
        color=TEXT_COLOR,
    )

ax.set_title("Monthly Revenue Trend (Jan\u2013May 2024)", fontsize=14, fontweight="bold")
ax.set_xlabel("Month")
ax.set_ylabel("Revenue ($)")

apply_paper_style(fig, ax)
fig.tight_layout()
out_path = OUTPUT_DIR / "line_monthly_trend.png"
save_styled(fig, out_path)
print(f"Saved: {out_path}\n")

# =============================================================================
# 4. Horizontal bar chart — top customers by total spend
# =============================================================================
# Horizontal bars are a great choice when category labels are long.
print("=== 4. Horizontal Bar Chart: Top Customers by Spend ===")

customer_spend = conn.sql(
    """
    SELECT
        customer,
        ROUND(SUM(quantity * price), 2) AS total_spend
    FROM sales
    GROUP BY customer
    ORDER BY total_spend ASC  -- ascending so the highest bar is at the top
    """
).df()

print(customer_spend.to_string(index=False))

fig, ax = plt.subplots(figsize=(8, 5))
bars = ax.barh(
    customer_spend["customer"],
    customer_spend["total_spend"],
    color=PASTEL_COLORS[:4],
    edgecolor=SPINE_COLOR,
    linewidth=0.8,
)

# Add value labels to the right of each bar
for bar in bars:
    width = bar.get_width()
    ax.text(
        width + 3,
        bar.get_y() + bar.get_height() / 2,
        f"${width:,.2f}",
        va="center",
        fontsize=10,
        color=TEXT_COLOR,
    )

ax.set_title("Total Spend by Customer", fontsize=14, fontweight="bold")
ax.set_xlabel("Total Spend ($)")
ax.set_ylabel("Customer")

apply_paper_style(fig, ax)
fig.tight_layout()
out_path = OUTPUT_DIR / "hbar_customer_spend.png"
save_styled(fig, out_path)
print(f"Saved: {out_path}\n")

# =============================================================================
# 5. Grouped bar chart — monthly revenue by product
# =============================================================================
# We use DuckDB to compute the cross-tab, then pandas .pivot() to reshape
# the long-form result into a wide-form table suitable for grouped bars.
print("=== 5. Grouped Bar Chart: Monthly Revenue by Product ===")

monthly_product = conn.sql(
    """
    SELECT
        date_trunc('month', date) AS month,
        product,
        ROUND(SUM(quantity * price), 2) AS revenue
    FROM sales
    GROUP BY month, product
    ORDER BY month, product
    """
).df()

# Pivot from long to wide: one column per product, one row per month
pivot = monthly_product.pivot(index="month", columns="product", values="revenue").fillna(0)
# DatetimeIndex has .strftime but ty doesn't see it through the generic Index type.
pivot.index = pd.DatetimeIndex(pivot.index).strftime("%b %Y")  # friendly labels

print(pivot.to_string())

# Plot grouped bars — pandas makes this easy via .plot(kind="bar")
fig, ax = plt.subplots(figsize=(10, 6))
pivot.plot(
    kind="bar",
    ax=ax,
    color=PASTEL_COLORS[:3],
    edgecolor=SPINE_COLOR,
    width=0.75,
    linewidth=0.8,
)

ax.set_title("Monthly Revenue by Product", fontsize=14, fontweight="bold")
ax.set_xlabel("Month")
ax.set_ylabel("Revenue ($)")
ax.legend(title="Product")
ax.set_xticklabels(pivot.index, rotation=0)  # keep labels horizontal

apply_paper_style(fig, ax)
fig.tight_layout()
out_path = OUTPUT_DIR / "grouped_bar_monthly_product.png"
save_styled(fig, out_path)
print(f"Saved: {out_path}\n")

# =============================================================================
# 6. Combined chart — bar + line overlay (revenue bars + cumulative line)
# =============================================================================
# A common dashboard pattern: bars show periodic values while a line shows
# the running total.  DuckDB window functions compute the cumulative sum.
print("=== 6. Combined Chart: Revenue Bars + Cumulative Line ===")

combined = conn.sql(
    """
    SELECT
        date_trunc('month', date) AS month,
        ROUND(SUM(quantity * price), 2) AS revenue,
        -- Running total via a window function — no Python code needed
        ROUND(
            SUM(SUM(quantity * price)) OVER (ORDER BY date_trunc('month', date)),
            2
        ) AS cumulative_revenue
    FROM sales
    GROUP BY month
    ORDER BY month
    """
).df()

combined["label"] = combined["month"].dt.strftime("%b %Y")

print(combined[["label", "revenue", "cumulative_revenue"]].to_string(index=False))

fig, ax1 = plt.subplots(figsize=(9, 6))

# --- Bars: monthly revenue on the primary y-axis ---
bar_color = PASTEL_COLORS[0]  # warm peach
line_color = PASTEL_COLORS[2]  # dusty rose

bar_x = range(len(combined))
ax1.bar(
    bar_x,
    combined["revenue"],
    color=bar_color,
    alpha=0.85,
    label="Monthly Revenue",
    width=0.6,
    edgecolor=SPINE_COLOR,
    linewidth=0.8,
)
ax1.set_xlabel("Month")
ax1.set_ylabel("Monthly Revenue ($)", color=TEXT_COLOR)
ax1.tick_params(axis="y", labelcolor=TEXT_COLOR)
ax1.set_xticks(list(bar_x))
ax1.set_xticklabels(combined["label"])

# --- Line: cumulative revenue on a secondary y-axis ---
ax2 = ax1.twinx()  # shares the same x-axis
ax2.plot(
    list(bar_x),
    combined["cumulative_revenue"],
    color=line_color,
    marker="D",
    linewidth=2.5,
    markersize=7,
    markeredgecolor=TEXT_COLOR,
    markeredgewidth=0.8,
    label="Cumulative Revenue",
)
ax2.set_ylabel("Cumulative Revenue ($)", color=TEXT_COLOR)
ax2.tick_params(axis="y", labelcolor=TEXT_COLOR)

# Annotate cumulative values
for i, val in enumerate(combined["cumulative_revenue"]):
    ax2.annotate(
        f"${val:,.2f}",
        xy=(i, val),
        textcoords="offset points",
        xytext=(0, 12),
        ha="center",
        fontsize=9,
        color=TEXT_COLOR,
    )

# Combine legends from both axes
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

ax1.set_title("Monthly Revenue with Cumulative Total", fontsize=14, fontweight="bold")

# Style both axes (primary and secondary)
apply_paper_style(fig, ax1, ax2)
fig.tight_layout()
out_path = OUTPUT_DIR / "combined_bar_line.png"
save_styled(fig, out_path)
print(f"Saved: {out_path}\n")

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
conn.close()
print("All charts saved to examples/output/. Done!")
