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
# 2. READ SILVER DATA
# ============================================================

silver_key = (
    "silver/supply_chain/"
    "supplyChain_Cleaned.parquet"
)

obj = s3.get_object(
    Bucket=bucket,
    Key=silver_key
)

df = pd.read_parquet(
    io.BytesIO(obj["Body"].read())
)

print(f"Silver rows read: {len(df)}")


# ============================================================
# 3. VALIDATE EXPECTED SILVER COLUMNS
# ============================================================

required_columns = [
    "order_id",
    "order_date",
    "expected_delivery",
    "actual_delivery",

    "supplier_id",
    "product_id",
    "warehouse_id",

    "quantity_ordered",
    "quantity_received",

    "unit_cost",
    "shipping_cost",

    "transport_mode",
    "order_status",
    "priority",

    "defect_count",

    "has_quality_issue",

    "delivery_delay_days",
    "lead_time_days",

    "procurement_cost",
    "total_cost"
]

missing_columns = [
    col
    for col in required_columns
    if col not in df.columns
]

if missing_columns:
    raise ValueError(
        "Required Silver columns are missing: "
        + str(missing_columns)
    )

print("Silver schema validation: PASSED")


# ============================================================
# 4. VALIDATE DIMENSION KEYS
# ============================================================

print("========== KEY VALIDATION ==========")

print(
    "Missing supplier_id:",
    df["supplier_id"].isna().sum()
)

print(
    "Missing product_id:",
    df["product_id"].isna().sum()
)

print(
    "Missing warehouse_id:",
    df["warehouse_id"].isna().sum()
)

print("====================================")


# These should all be 0 based on our Silver dataset
# and the corrections made in Gold Dimensions.

if df["supplier_id"].isna().any():
    raise ValueError(
        "supplier_id contains NULL values."
    )

if df["product_id"].isna().any():
    raise ValueError(
        "product_id contains NULL values."
    )

if df["warehouse_id"].isna().any():
    raise ValueError(
        "warehouse_id contains NULL values."
    )


# ============================================================
# 5. STANDARDIZE ORDER DATE
# ============================================================

df["order_date"] = pd.to_datetime(
    df["order_date"],
    errors="coerce"
)

print(
    "Rows with invalid/missing order_date:",
    df["order_date"].isna().sum()
)


# ============================================================
# 6. CREATE DATE KEY
# ============================================================

df["date_id"] = (
    df["order_date"]
    .dt.strftime("%Y%m%d")
)

df["date_id"] = pd.to_numeric(
    df["date_id"],
    errors="coerce"
).astype("Int64")


# ============================================================
# 7. CALCULATE DEFECT RATE
# ============================================================

# Prevent division by zero when quantity_received = 0

received_quantity = (
    df["quantity_received"]
    .replace(0, pd.NA)
)

df["defect_rate"] = (
    df["defect_count"]
    / received_quantity
)

df["defect_rate"] = (
    df["defect_rate"]
    .fillna(0)
)


# ============================================================
# 8. CREATE FACT TABLE
# ============================================================

fact_procurement = df[
    [
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
].copy()


# ============================================================
# 9. FACT GRAIN VALIDATION
# ============================================================

print("========== FACT VALIDATION ==========")

print(
    "Fact rows:",
    len(fact_procurement)
)

print(
    "Unique order IDs:",
    fact_procurement["order_id"].nunique()
)

duplicate_orders = (
    fact_procurement["order_id"]
    .duplicated()
    .sum()
)

print(
    "Duplicate order IDs:",
    duplicate_orders
)

print("====================================")


# ============================================================
# 10. FOREIGN KEY VALIDATION
# ============================================================

print("========== FOREIGN KEY VALIDATION ==========")

print(
    "Null supplier IDs:",
    fact_procurement["supplier_id"].isna().sum()
)

print(
    "Null product IDs:",
    fact_procurement["product_id"].isna().sum()
)

print(
    "Null warehouse IDs:",
    fact_procurement["warehouse_id"].isna().sum()
)

print(
    "Null date IDs:",
    fact_procurement["date_id"].isna().sum()
)

print("============================================")


# ============================================================
# 11. BUSINESS METRICS VALIDATION
# ============================================================

print("========== BUSINESS METRICS ==========")

print(
    "Total quantity ordered:",
    f"{fact_procurement['quantity_ordered'].sum():,.0f}"
)

print(
    "Total quantity received:",
    f"{fact_procurement['quantity_received'].sum():,.0f}"
)

print(
    "Total procurement cost:",
    f"{fact_procurement['procurement_cost'].sum():,.2f}"
)

print(
    "Total shipping cost:",
    f"{fact_procurement['shipping_cost'].sum():,.2f}"
)

print(
    "Total cost:",
    f"{fact_procurement['total_cost'].sum():,.2f}"
)

print(
    "Total defects:",
    f"{fact_procurement['defect_count'].sum():,.0f}"
)

print(
    "Orders with quality issues:",
    fact_procurement["has_quality_issue"].sum()
)

print("=======================================")


# ============================================================
# 12. WRITE FACT TABLE TO GOLD
# ============================================================

def write_parquet(dataframe, bucket_name, key):

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


gold_key = (
    "gold/facts/"
    "fact_procurement/"
    "fact_procurement.parquet"
)

write_parquet(
    fact_procurement,
    bucket,
    gold_key
)


# ============================================================
# 13. FINAL OUTPUT
# ============================================================

print(
    "========== GOLD FACT JOB COMPLETED =========="
)

print(
    f"Final fact rows: {len(fact_procurement)}"
)

print(
    f"Output: s3://{bucket}/{gold_key}"
)