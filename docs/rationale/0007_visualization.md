# Track 7 — Visualization: Rationale

## Intention

Track 7 bridges the gap between "I can query data" and "I can present data."
DuckDB does the analytical heavy lifting — aggregation, window functions,
PIVOT, date truncation — and then a Python visualization library renders the
result. The learner sees that DuckDB + matplotlib/plotly/seaborn is a complete
analytics workflow with no database server, no heavy frameworks, and no data
movement.

A secondary goal is aesthetics: every chart uses a consistent warm, creamy,
paper-tinted visual style with pastel data colors. This teaches learners that
chart styling is part of the deliverable, not an afterthought.

## Why these four examples, in this order

### 71 — Matplotlib bar & line charts

Matplotlib is the foundation of the Python visualization stack. Every learner
should know it because seaborn, pandas `.plot()`, and many other libraries are
built on top of it.

- **Bar chart** is the simplest and most universal chart type. Revenue by
  product answers an obvious business question.
- **Line chart** introduces the time axis. `date_trunc('month', date)` is a
  DuckDB pattern that appears in almost every dashboard query.
- **Horizontal bar chart** shows when to rotate orientation (long labels) and
  introduces the `barh()` API.
- **Grouped bar chart** combines DuckDB cross-tabs with pandas `.pivot()` and
  matplotlib's grouped bar API — a common multi-dimensional reporting pattern.
- **Combined bar + line** is the most complex chart: dual y-axes, two data
  series, and DuckDB window functions computing the cumulative total. This is
  a real dashboard pattern (periodic vs running total).

The paper-style helper functions (`apply_paper_style`, `save_styled`) are
defined here first and demonstrate how to theme matplotlib programmatically:
custom facecolors, spine colors, grid styles, and text colors.

### 72 — Plotly interactive charts

Plotly Express is the fastest path to interactive charts. Hover, zoom, and pan
come for free — critical for exploratory analysis.

- **Bar chart** is repeated to contrast Plotly's API with matplotlib's. Same
  data, same SQL, different rendering paradigm.
- **Line chart** adds markers and interaction (hover values) that static
  matplotlib charts can't provide.
- **Scatter plot** introduces a third dimension (color by product) and shows
  hover data enrichment — a key Plotly feature.
- **Stacked bar** shows `barmode="stack"` and how DuckDB's two-column GROUP BY
  naturally produces the long-format data that Plotly expects.
- **Sunburst** is unique to Plotly. It visualises hierarchical part-to-whole
  relationships (customer → product) and teaches the `path=` / `values=`
  interface.

All charts use `fig.write_html()` instead of `fig.show()` — the script works
headlessly and the HTML files can be opened in any browser.

The warm paper-tinted theme is applied through a shared `apply_paper_layout`
helper that sets `paper_bgcolor`, `plot_bgcolor`, grid colors, font colors,
and the pastel color sequence.

### 73 — Seaborn statistical plots

Seaborn excels at statistical charts: automatic aggregation, error bars,
faceting, and heatmaps. It's seaborn that brings the "statistical" in
"statistical visualization."

- **Bar plot with error bars** shows that seaborn computes the mean and
  standard deviation automatically from raw rows — the learner only supplies
  `estimator="mean"` and `errorbar="sd"`.
- **Heatmap** is a showcase for DuckDB's PIVOT. The SQL reshapes the data into
  a matrix; `sns.heatmap()` renders it with annotations and a warm color ramp.
- **Box plot + strip overlay** combines two plot types on one axis, revealing
  both the distribution summary and individual data points.
- **Strip plot** with hue/dodge teaches the concept of encoding a third
  variable (product) via color within a categorical axis (customer).
- **Catplot** (faceted bars) demonstrates seaborn's FacetGrid: one subplot per
  quarter, with DuckDB computing the quarter label in SQL.

The paper theme is applied via `sns.set_theme()` with custom `rc` overrides
for facecolor, text color, spine color, and grid styling — showing learners
how to configure seaborn's theming system.

### 74 — Chart-ready data with PIVOT

This example has no visualization library at all. Its purpose is to prepare
DuckDB query results for consumption by external BI tools (Excel, Tableau,
Power BI, Google Sheets).

- **Wide format with PIVOT** creates a cross-tab table (products as columns,
  months as rows) — the format Excel pivot tables produce.
- **Long format with UNPIVOT** reverses the transformation — Tableau and
  Power BI prefer one-observation-per-row (tidy data).
- **Summary statistics** generates a KPI-card-ready table with totals,
  averages, min/max, and counts.
- **Time-series rollups** at daily, weekly, and monthly granularity show how
  to pre-aggregate data for different dashboard time filters.
- **Star schema** builds dimension and fact tables with surrogate keys — the
  standard modeling pattern for BI tools. This bridges SQL analytics with
  data warehouse design.

## Visual style: warm paper-tinted aesthetic

All matplotlib and seaborn charts share a consistent visual language:

| Element          | Color     | Hex       |
|------------------|-----------|-----------|
| Figure bg        | warm cream | `#fdf6ec` |
| Axes bg          | soft ivory | `#fefaf2` |
| Text & ticks     | warm brown | `#5c4b3a` |
| Spines & grid    | parchment  | `#d4c4a8` |
| Data palette     | 8 pastels  | peach, teal, rose, sage, gold, slate, lavender, salmon |

The Plotly charts use equivalent colors through `paper_bgcolor`, `plot_bgcolor`,
and `color_discrete_sequence`.

Design rationale:
- **Warm cream backgrounds** reduce visual harshness compared to pure white,
  making charts more pleasant to read for extended periods.
- **Pastel data colors** maintain distinguishability while keeping the overall
  tone soft and cohesive.
- **Consistent theming** across all three libraries teaches that branding and
  style consistency matter in professional data deliverables.

## Design decisions

- **Headless-first** — `matplotlib.use("Agg")` is set before any pyplot import.
  `plt.savefig()` and `fig.write_html()` replace `plt.show()` and `fig.show()`.
  Every script works in CI, SSH, and container environments.

- **Output to `examples/output/`** — a gitignored directory so generated images
  and HTML files don't bloat the repository.

- **SQL does the heavy lifting** — every chart starts with a DuckDB query that
  handles aggregation, grouping, date truncation, PIVOT, or window functions.
  The Python visualization code is minimal — typically under 10 lines per chart.

- **Three libraries, not one** — matplotlib for control, plotly for
  interactivity, seaborn for statistics. Each library has a distinct strength.
  Showing all three helps the learner choose the right tool for the job.

- **74 stands alone** — the chart-ready data example intentionally avoids any
  visualization library. It demonstrates that "data preparation for charts" is
  a skill in its own right, and that DuckDB's SQL features (PIVOT, UNPIVOT,
  window functions, star-schema modeling) are the real engine behind good
  dashboards.
