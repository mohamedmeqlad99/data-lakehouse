#!/usr/bin/env python3
import os
from pathlib import Path

import pandas as pd
import streamlit as st
from trino.dbapi import connect

st.set_page_config(
    page_title="Lakehouse Operations",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    :root {
        --ink: #142c35;
        --muted: #64777e;
        --paper: #f2f5f1;
        --panel: #ffffff;
        --teal: #087e83;
        --lime: #d4ee71;
        --line: #d9e2dc;
        --rust: #b84c35;
    }
    .stApp { background: var(--paper); color: var(--ink); }
    [data-testid="stHeader"] { background: transparent; }
    .block-container { max-width: 1440px; padding-top: 2rem; }
    h1, h2, h3, p, label { color: var(--ink); }
    .eyebrow {
        color: var(--teal); font-size: .76rem; font-weight: 800;
        letter-spacing: .13em; text-transform: uppercase; margin-bottom: .4rem;
    }
    .hero {
        background: linear-gradient(112deg, #153944 0%, #087e83 70%, #2a9991 100%);
        color: #f7fbf8; border-radius: 10px; padding: 1.35rem 1.6rem;
        margin: .6rem 0 1.25rem;
    }
    .hero h1 { color: #fff; margin: 0; font-size: 2rem; }
    .hero p { color: #d8eeea; margin: .35rem 0 0; }
    .kpi-label { color: var(--muted); font-size: .76rem; font-weight: 800; letter-spacing: .08em; text-transform: uppercase; }
    .kpi-value { color: var(--ink); font-size: 1.85rem; font-weight: 750; line-height: 1.2; margin-top: .45rem; }
    .kpi-detail { color: var(--muted); font-size: .82rem; margin-top: .15rem; }
    div[data-testid="stMetric"] {
        background: var(--panel); padding: 1rem 1.1rem; border: 1px solid var(--line);
        border-radius: 9px; min-height: 114px;
    }
    div[data-testid="stMetricLabel"] p { color: var(--muted); font-weight: 700; }
    div[data-testid="stMetricValue"] { color: var(--ink); }
    div[data-testid="stDataFrame"] { border: 1px solid var(--line); border-radius: 8px; }
    .section-head { margin: 1.35rem 0 .15rem; }
    .section-note { color: var(--muted); font-size: .88rem; margin-bottom: .8rem; }
    .status-good { color: var(--teal); font-weight: 700; }
    .status-warn { color: var(--rust); font-weight: 700; }
    [data-testid="stSidebar"] { background: #e7eeea; }
    [data-testid="stSidebar"] h2 { font-size: 1rem; }
    button[kind="primary"] { background: var(--teal); border-color: var(--teal); }
    </style>
    """,
    unsafe_allow_html=True,
)


def run_trino(sql: str) -> list[dict[str, str]]:
    connection = connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "lakehouse-dashboard"),
        catalog="iceberg",
        schema="default",
        http_scheme="http",
    )
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        columns = [column[0] for column in cursor.description or []]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        connection.close()


@st.cache_data(ttl=15, show_spinner=False)
def load_dashboard_data() -> dict:
    return {
        "schemas": run_trino("SHOW SCHEMAS"),
        "silver_tables": run_trino("SHOW TABLES FROM silver"),
        "inventory": run_trino(
            "SELECT inventory_id, product_id, warehouse, quantity, last_event_id, updated_at "
            "FROM silver.inventory ORDER BY warehouse, inventory_id"
        ),
        "revenue": run_trino(
            "SELECT region, ROUND(SUM(revenue), 2) AS revenue, SUM(order_count) AS orders "
            "FROM gold.daily_revenue GROUP BY region ORDER BY revenue DESC"
        ),
        "quality": run_trino(
            "SELECT dataset, metric, issue_count "
            "FROM gold.data_quality_summary ORDER BY dataset, metric"
        ),
        "snapshots": run_trino(
            'SELECT committed_at, snapshot_id, operation '
            'FROM silver."inventory$snapshots" ORDER BY committed_at DESC LIMIT 5'
        ),
    }


def to_frame(rows: list[dict[str, str]]) -> pd.DataFrame:
    return pd.DataFrame(rows) if rows else pd.DataFrame()


with st.sidebar:
    st.markdown("## Lakehouse controls")
    st.caption("Spark · Iceberg · MinIO · Trino")
    refresh = st.button("Refresh live data", type="primary", use_container_width=True)
    if refresh:
        load_dashboard_data.clear()
        st.rerun()
    st.caption("Metrics refresh from Trino every 15 seconds.")
    st.divider()
    st.markdown("**Services**")
    st.markdown("- MinIO: `localhost:9000`")
    st.markdown("- Iceberg REST: `localhost:8181`")
    st.markdown("- Trino: `localhost:8088`")
    st.markdown("- Spark UI: `localhost:8080`")

st.markdown(
    """
    <div class="hero">
      <div class="eyebrow" style="color:#d4ee71">Lakehouse · Operations</div>
      <h1>Data at a glance</h1>
      <p>Live inventory, commercial performance, and quality signals from the Iceberg warehouse.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

try:
    data = load_dashboard_data()
except Exception as exc:
    st.error("Could not load live metrics from Trino.")
    st.code(str(exc))
    st.info("Start the lakehouse services and run `make bootstrap`, then refresh this dashboard.")
    st.stop()

schemas = to_frame(data["schemas"])
tables = to_frame(data["silver_tables"])
inventory = to_frame(data["inventory"])
revenue = to_frame(data["revenue"])
quality = to_frame(data["quality"])
snapshots = to_frame(data["snapshots"])

inventory["quantity"] = pd.to_numeric(inventory.get("quantity", pd.Series(dtype=float)), errors="coerce")
revenue["revenue"] = pd.to_numeric(revenue.get("revenue", pd.Series(dtype=float)), errors="coerce")
revenue["orders"] = pd.to_numeric(revenue.get("orders", pd.Series(dtype=float)), errors="coerce")
quality["issue_count"] = pd.to_numeric(quality.get("issue_count", pd.Series(dtype=float)), errors="coerce").fillna(0)

total_revenue = revenue["revenue"].sum() if not revenue.empty else 0
order_count = int(revenue["orders"].sum()) if not revenue.empty else 0
quality_issues = int(quality["issue_count"].sum()) if not quality.empty else 0
low_stock = int((inventory["quantity"] <= 10).sum()) if not inventory.empty else 0

kpis = st.columns(4)
kpis[0].metric("Revenue", f"${total_revenue:,.2f}", help="Revenue summed across the Gold daily revenue mart.")
kpis[1].metric("Orders", f"{order_count:,}", help="Orders represented in the Gold daily revenue mart.")
kpis[2].metric("Low / out of stock", f"{low_stock:,}", help="Inventory rows at or below 10 units.")
kpis[3].metric("Quality issues", f"{quality_issues:,}", help="Sum of issue counts from the Gold quality summary.")

left, right = st.columns([1.2, 0.8], gap="large")
with left:
    st.markdown('<div class="section-head"><h2>Inventory position</h2></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-note">Latest committed quantities by warehouse and product.</div>', unsafe_allow_html=True)
    if inventory.empty:
        st.info("No inventory rows are available.")
    else:
        display_inventory = inventory.rename(
            columns={
                "inventory_id": "Inventory ID",
                "product_id": "Product ID",
                "warehouse": "Warehouse",
                "quantity": "Quantity",
                "last_event_id": "Last Event",
                "updated_at": "Updated At",
            }
        )
        st.dataframe(
            display_inventory,
            use_container_width=True,
            hide_index=True,
            column_config={"Quantity": st.column_config.NumberColumn(format="%d units")},
        )

with right:
    st.markdown('<div class="section-head"><h2>Revenue by region</h2></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-note">Gold daily revenue aggregated across dates.</div>', unsafe_allow_html=True)
    if revenue.empty:
        st.info("No revenue rows are available.")
    else:
        st.bar_chart(revenue.set_index("region")["revenue"], color="#087e83")
        st.dataframe(
            revenue.rename(columns={"region": "Region", "revenue": "Revenue", "orders": "Orders"}),
            use_container_width=True,
            hide_index=True,
            column_config={"Revenue": st.column_config.NumberColumn(format="$%.2f")},
        )

lower_left, lower_right = st.columns([1, 1], gap="large")
with lower_left:
    st.markdown('<div class="section-head"><h2>Quality checks</h2></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-note">Non-zero metrics need attention; zero means the check passed.</div>', unsafe_allow_html=True)
    if quality.empty:
        st.info("No quality checks are available.")
    else:
        visible_quality = quality[quality["issue_count"] > 0]
        if visible_quality.empty:
            st.success("All reported quality checks are clear.")
        else:
            st.dataframe(
                visible_quality.rename(columns={"dataset": "Dataset", "metric": "Check", "issue_count": "Issues"}),
                use_container_width=True,
                hide_index=True,
            )

with lower_right:
    st.markdown('<div class="section-head"><h2>Inventory snapshots</h2></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-note">Recent Iceberg commits for the inventory table.</div>', unsafe_allow_html=True)
    if snapshots.empty:
        st.info("No inventory snapshots are available.")
    else:
        st.dataframe(
            snapshots.rename(columns={"committed_at": "Committed At", "snapshot_id": "Snapshot ID", "operation": "Operation"}),
            use_container_width=True,
            hide_index=True,
        )

with st.expander("Warehouse structure"):
    structure_left, structure_right = st.columns(2)
    with structure_left:
        st.markdown("**Schemas**")
        st.write(", ".join(schemas.iloc[:, 0].astype(str).tolist()) if not schemas.empty else "None")
    with structure_right:
        st.markdown(f"**Silver tables ({len(tables)})**")
        st.write(", ".join(tables.iloc[:, 0].astype(str).tolist()) if not tables.empty else "None")

st.caption("Read-only dashboard · source: Trino Iceberg catalog · refresh interval: 15 seconds")
