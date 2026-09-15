import sys
import io
import boto3
import pandas as pd

from awsglue.utils import getResolvedOptions


# ============================================================
# 1. GET JOB PARAMETERS
# ============================================================

args = getResolvedOptions(
    sys.argv,
    ["bucket"]
)

bucket = args["bucket"]

s3 = boto3.client("s3")


# ============================================================
# 2. READ GOLD FACT
# ============================================================

fact_key = (
    "gold/facts/"
    "fact_procurement/"
    "fact_procurement.parquet"
)

obj = s3.get_object(
    Bucket=bucket,
    Key=fact_key
)

fact = pd.read_parquet(
    io.BytesIO(obj["Body"].read())
)

print(
    f"Gold Fact rows read: {len(fact)}"
)


# ============================================================
# 3. VALIDATE FACT SCHEMA
# ============================================================

required_columns = [
    "order_id",
    "order_date",
    "date_id",

    "supplier_id",
    "product_id",
    "warehouse_id",

    "quantity_ordered",
    "quantity_received",

    "unit_cost",
    "procurement_cost",
    "shipping_cost",
    "total_cost",

    "expected_delivery",
    "actual_delivery",

    "delivery_delay_days",
    "lead_time_days",

    "defect_count",
    "defect_rate",

    "transport_mode",
    "order_status",
    "priority",

    "has_quality_issue"
]

missing_columns = [
    col
    for col in required_columns
    if col not in fact.columns
]

if missing_columns:

    raise ValueError(
        "Required Fact columns are missing: "
        + str(missing_columns)
    )

print(
    "Gold Fact schema validation: PASSED"
)


# ============================================================
# 4. STANDARDIZE DATES
# ============================================================

fact["order_date"] = pd.to_datetime(
    fact["order_date"],
    errors="coerce"
)


# ============================================================
# 5. SUPPLIER PERFORMANCE
# ============================================================

agg_supplier_performance = (
    fact
    .groupby("supplier_id", dropna=False)
    .agg(
        total_orders=(
            "order_id",
            "nunique"
        ),

        total_quantity_ordered=(
            "quantity_ordered",
            "sum"
        ),

        total_quantity_received=(
            "quantity_received",
            "sum"
        ),

        total_procurement_cost=(
            "procurement_cost",
            "sum"
        ),

        total_shipping_cost=(
            "shipping_cost",
            "sum"
        ),

        total_cost=(
            "total_cost",
            "sum"
        ),

        total_defects=(
            "defect_count",
            "sum"
        ),

        avg_defect_rate=(
            "defect_rate",
            "mean"
        ),

        avg_delivery_delay_days=(
            "delivery_delay_days",
            "mean"
        ),

        avg_lead_time_days=(
            "lead_time_days",
            "mean"
        ),

        quality_issue_orders=(
            "has_quality_issue",
            "sum"
        )
    )
    .reset_index()
)


# Fill metrics where no valid delivery delay exists.

agg_supplier_performance[
    "avg_delivery_delay_days"
] = (
    agg_supplier_performance[
        "avg_delivery_delay_days"
    ].fillna(0)
)

agg_supplier_performance[
    "avg_lead_time_days"
] = (
    agg_supplier_performance[
        "avg_lead_time_days"
    ].fillna(0)
)

# ============================================================
# 6. CALCULATED SUPPLIER KPIs
# ============================================================

agg_supplier_performance[
    "fulfillment_rate"
] = (
    agg_supplier_performance[
        "total_quantity_received"
    ]
    /
    agg_supplier_performance[
        "total_quantity_ordered"
    ].replace(0, pd.NA)
)

agg_supplier_performance[
    "fulfillment_rate"
] = (
    agg_supplier_performance[
        "fulfillment_rate"
    ].fillna(0)
)


agg_supplier_performance[
    "quality_issue_rate"
] = (
    agg_supplier_performance[
        "quality_issue_orders"
    ]
    /
    agg_supplier_performance[
        "total_orders"
    ].replace(0, pd.NA)
)

agg_supplier_performance[
    "quality_issue_rate"
] = (
    agg_supplier_performance[
        "quality_issue_rate"
    ].fillna(0)
)


# ============================================================
# 7. WAREHOUSE PERFORMANCE
# ============================================================

agg_warehouse_performance = (
    fact
    .groupby("warehouse_id", dropna=False)
    .agg(
        total_orders=(
            "order_id",
            "nunique"
        ),

        total_quantity_ordered=(
            "quantity_ordered",
            "sum"
        ),

        total_quantity_received=(
            "quantity_received",
            "sum"
        ),

        total_cost=(
            "total_cost",
            "sum"
        ),

        total_shipping_cost=(
            "shipping_cost",
            "sum"
        ),

        total_defects=(
            "defect_count",
            "sum"
        ),

        avg_defect_rate=(
            "defect_rate",
            "mean"
        ),

        avg_delivery_delay_days=(
            "delivery_delay_days",
            "mean"
        ),

        quality_issue_orders=(
            "has_quality_issue",
            "sum"
        )
    )
    .reset_index()
)


agg_warehouse_performance[
    "fulfillment_rate"
] = (
    agg_warehouse_performance[
        "total_quantity_received"
    ]
    /
    agg_warehouse_performance[
        "total_quantity_ordered"
    ].replace(0, pd.NA)
)

agg_warehouse_performance[
    "fulfillment_rate"
] = (
    agg_warehouse_performance[
        "fulfillment_rate"
    ].fillna(0)
)


# ============================================================
# 8. MONTHLY PROCUREMENT
# ============================================================

monthly_fact = fact[
    fact["order_date"].notna()
].copy()

monthly_fact["order_month"] = (
    monthly_fact["order_date"]
    .dt.to_period("M")
    .astype(str)
)


agg_monthly_procurement = (
    monthly_fact
    .groupby("order_month")
    .agg(
        total_orders=(
            "order_id",
            "nunique"
        ),

        total_quantity_ordered=(
            "quantity_ordered",
            "sum"
        ),

        total_quantity_received=(
            "quantity_received",
            "sum"
        ),

        total_procurement_cost=(
            "procurement_cost",
            "sum"
        ),

        total_shipping_cost=(
            "shipping_cost",
            "sum"
        ),

        total_cost=(
            "total_cost",
            "sum"
        ),

        total_defects=(
            "defect_count",
            "sum"
        ),

        avg_delivery_delay_days=(
            "delivery_delay_days",
            "mean"
        ),

        quality_issue_orders=(
            "has_quality_issue",
            "sum"
        )
    )
    .reset_index()
)


agg_monthly_procurement[
    "fulfillment_rate"
] = (
    agg_monthly_procurement[
        "total_quantity_received"
    ]
    /
    agg_monthly_procurement[
        "total_quantity_ordered"
    ].replace(0, pd.NA)
)

agg_monthly_procurement[
    "fulfillment_rate"
] = (
    agg_monthly_procurement[
        "fulfillment_rate"
    ].fillna(0)
)


# ============================================================
# 9. TRANSPORT PERFORMANCE
# ============================================================

agg_transport_performance = (
    fact
    .groupby("transport_mode", dropna=False)
    .agg(
        total_orders=(
            "order_id",
            "nunique"
        ),

        total_quantity_ordered=(
            "quantity_ordered",
            "sum"
        ),

        total_quantity_received=(
            "quantity_received",
            "sum"
        ),

        total_shipping_cost=(
            "shipping_cost",
            "sum"
        ),

        total_cost=(
            "total_cost",
            "sum"
        ),

        avg_delivery_delay_days=(
            "delivery_delay_days",
            "mean"
        ),

        avg_lead_time_days=(
            "lead_time_days",
            "mean"
        ),

        total_defects=(
            "defect_count",
            "sum"
        ),

        quality_issue_orders=(
            "has_quality_issue",
            "sum"
        )
    )
    .reset_index()
)


agg_transport_performance[
    "fulfillment_rate"
] = (
    agg_transport_performance[
        "total_quantity_received"
    ]
    /
    agg_transport_performance[
        "total_quantity_ordered"
    ].replace(0, pd.NA)
)

agg_transport_performance[
    "fulfillment_rate"
] = (
    agg_transport_performance[
        "fulfillment_rate"
    ].fillna(0)
)


# ============================================================
# 10. WRITE PARQUET FUNCTION
# ============================================================

def write_parquet(
    dataframe,
    bucket_name,
    key
):

    buffer = io.BytesIO()

    dataframe.to_parquet(
        buffer,
        index=False
    )

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=buffer.getvalue()
    )

    print(
        f"Written successfully to: "
        f"s3://{bucket_name}/{key}"
    )


# ============================================================
# 11. WRITE SUPPLIER AGGREGATION
# ============================================================

write_parquet(
    agg_supplier_performance,
    bucket,
    "gold/aggregations/"
    "agg_supplier_performance/"
    "agg_supplier_performance.parquet"
)


# ============================================================
# 12. WRITE WAREHOUSE AGGREGATION
# ============================================================

write_parquet(
    agg_warehouse_performance,
    bucket,
    "gold/aggregations/"
    "agg_warehouse_performance/"
    "agg_warehouse_performance.parquet"
)


# ============================================================
# 13. WRITE MONTHLY AGGREGATION
# ============================================================

write_parquet(
    agg_monthly_procurement,
    bucket,
    "gold/aggregations/"
    "agg_monthly_procurement/"
    "agg_monthly_procurement.parquet"
)


# ============================================================
# 14. WRITE TRANSPORT AGGREGATION
# ============================================================

write_parquet(
    agg_transport_performance,
    bucket,
    "gold/aggregations/"
    "agg_transport_performance/"
    "agg_transport_performance.parquet"
)


# ============================================================
# 15. FINAL OUTPUT
# ============================================================

print(
    "========== GOLD AGGREGATION JOB COMPLETED =========="
)

print(
    "Supplier aggregation rows:",
    len(agg_supplier_performance)
)

print(
    "Warehouse aggregation rows:",
    len(agg_warehouse_performance)
)

print(
    "Monthly aggregation rows:",
    len(agg_monthly_procurement)
)

print(
    "Transport aggregation rows:",
    len(agg_transport_performance)
)