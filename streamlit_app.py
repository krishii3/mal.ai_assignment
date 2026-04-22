"""Streamlit dashboard — Unified Payment Data Pipeline.

Run:
    streamlit run streamlit_app.py

Reads the v2 canonical Parquet and renders KPIs + charts powered by DuckDB.
"""
from __future__ import annotations

from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent
V2 = ROOT / "data" / "output" / "unified_payments_v2.parquet"
QUERY_FILE = ROOT / "sql" / "analytical_queries.sql"

PRESET_QUERIES = [
    {
        "name": "Q1. Daily payment volume",
        "use_case": "Finance daily P&L and payment trend review.",
        "sql": f"""
SELECT
    CAST(timestamp AS DATE) AS txn_date,
    payment_type,
    COUNT(*) AS txn_count,
    ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS total_amount_aed
FROM '{V2.as_posix()}'
WHERE currency = 'AED'
GROUP BY txn_date, payment_type
ORDER BY txn_date DESC, payment_type
""".strip(),
    },
    {
        "name": "Q2. Failure rate by source",
        "use_case": "Operations health and squad-level incident detection.",
        "sql": f"""
SELECT
    source_system,
    COUNT(*) AS total,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    ROUND(100.0 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_pct
FROM '{V2.as_posix()}'
GROUP BY source_system
ORDER BY failure_pct DESC
""".strip(),
    },
    {
        "name": "Q3. Top bill categories",
        "use_case": "Biller concentration and utility-payment mix analysis.",
        "sql": f"""
SELECT
    payment_metadata->>'biller_category' AS biller_category,
    COUNT(*) AS payments,
    ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS total_aed
FROM '{V2.as_posix()}'
WHERE payment_type = 'BILL_PAYMENT'
GROUP BY biller_category
ORDER BY total_aed DESC
LIMIT 10
""".strip(),
    },
    {
        "name": "Q4. Customer payment frequency",
        "use_case": "Customer segmentation and high-frequency payer analysis.",
        "sql": f"""
SELECT
    customer_id,
    COUNT(*) AS payment_count,
    ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS total_aed,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS frequency_rank
FROM '{V2.as_posix()}'
WHERE status = 'COMPLETED' AND currency = 'AED'
GROUP BY customer_id
ORDER BY frequency_rank
LIMIT 10
""".strip(),
    },
    {
        "name": "Q5. Schema version rollout",
        "use_case": "Contract migration monitoring during cutover.",
        "sql": f"""
SELECT
    schema_version,
    COUNT(*) AS events,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM '{V2.as_posix()}'
GROUP BY schema_version
""".strip(),
    },
    {
        "name": "Q6. Currency mix",
        "use_case": "FX exposure review for treasury and risk.",
        "sql": f"""
SELECT
    currency,
    COUNT(*) AS txn_count,
    ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS total_in_native_ccy
FROM '{V2.as_posix()}'
GROUP BY currency
ORDER BY txn_count DESC
""".strip(),
    },
]

st.set_page_config(page_title="Mal — Unified Payments", page_icon="💳", layout="wide")
st.title("Mal — Unified Payment Data Pipeline")
st.caption("Canonical payment events across Cards, Transfers, and Bill Payments squads.")

if not V2.exists():
    st.error(f"Parquet not found at {V2}. Run `python -m src.main` first.")
    st.stop()

con = duckdb.connect()
df = con.sql(f"SELECT * FROM '{V2.as_posix()}'").df()
df["amount"] = pd.to_numeric(df["amount"])
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

# ── KPI cards ─────────────────────────────────────────────────────────
aed = df[df["currency"] == "AED"]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total events", f"{len(df):,}")
c2.metric("AED volume", f"{aed['amount'].sum():,.2f}")
c3.metric(
    "Failure rate",
    f"{100 * (df['status'] == 'FAILED').mean():.2f}%",
)
c4.metric("Schema version", df["schema_version"].mode().iat[0])

# ── Charts ────────────────────────────────────────────────────────────
left, right = st.columns(2)

with left:
    st.subheader("Payment type mix")
    mix = df.groupby("payment_type").size().reset_index(name="count")
    chart = (
        alt.Chart(mix)
        .mark_arc(innerRadius=60)
        .encode(theta="count:Q", color="payment_type:N", tooltip=["payment_type", "count"])
    )
    st.altair_chart(chart, use_container_width=True)

with right:
    st.subheader("Status breakdown by source")
    stat = df.groupby(["source_system", "status"]).size().reset_index(name="count")
    chart = (
        alt.Chart(stat)
        .mark_bar()
        .encode(x="source_system:N", y="count:Q", color="status:N", tooltip=["source_system", "status", "count"])
    )
    st.altair_chart(chart, use_container_width=True)

st.subheader("Daily AED volume")
daily = (
    aed.assign(date=aed["timestamp"].dt.date)
    .groupby(["date", "payment_type"])["amount"]
    .sum()
    .reset_index()
)
chart = (
    alt.Chart(daily)
    .mark_line(point=True)
    .encode(
        x="date:T",
        y=alt.Y("amount:Q", title="AED"),
        color="payment_type:N",
        tooltip=["date", "payment_type", "amount"],
    )
)
st.altair_chart(chart, use_container_width=True)

with st.expander("Sample canonical records"):
    st.dataframe(df.head(20), use_container_width=True)

st.subheader("Analytical query explorer")
st.caption("Preloaded DuckDB queries from the assignment, exposed in the GUI for interview walkthroughs.")

selected_name = st.selectbox("Choose a query", [q["name"] for q in PRESET_QUERIES])
selected_query = next(q for q in PRESET_QUERIES if q["name"] == selected_name)

query_left, query_right = st.columns([1, 1])
with query_left:
    st.markdown(f"**Business use:** {selected_query['use_case']}")
    st.code(selected_query["sql"], language="sql")
with query_right:
    st.markdown("**Result preview**")
    st.dataframe(con.sql(selected_query["sql"]).df(), use_container_width=True)

with st.expander("View full SQL file"):
    st.code(QUERY_FILE.read_text(), language="sql")

with st.expander("Run custom SQL (DuckDB)"):
    default_q = (
        f"SELECT payment_type, COUNT(*) AS n FROM '{V2.as_posix()}' GROUP BY 1 ORDER BY n DESC"
    )
    q = st.text_area("Query", default_q, height=100)
    if st.button("Run"):
        try:
            st.dataframe(con.sql(q).df(), use_container_width=True)
        except Exception as exc:  # pragma: no cover
            st.error(str(exc))
