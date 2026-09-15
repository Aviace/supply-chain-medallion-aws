import sys
import io
import boto3
import pandas as pd

from datetime import datetime

from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ["bucket"]
)

bucket = args["bucket"]

s3 = boto3.client("s3")

silver_key = (
    "silver/supply_chain/supplyChain_Cleaned.parquet"
)

obj = s3.get_object(
    Bucket = bucket,
    Key = silver_key
)

df = pd.read_parquet(
    io.BytesIO(obj["Body"].read())
)

print(f"Silver rows read: {len(df)}")

#dim_supplier
dim_supplier = (
    df[
        [
            "supplier_id",
            "supplier_name",
            "supplier_country"
        ]
    ]
    .drop_duplicates()
    .sort_values("supplier_id")
    .reset_index(drop=True)
)

#dim_product
dim_product = (
    df[
        [
            "product_id",
            "product_name",
            "category"
        ]
    ]
    .drop_duplicates()
    .sort_values("product_id")
    .reset_index(drop=True)
)

#dim_warehouse
dim_warehouse = (
    df[
        [
            "warehouse_id",
            "warehouse_city"
        ]
    ]
    .drop_duplicates()
    .sort_values("warehouse_id")
    .reset_index(drop=True)
)

#create dim_date
#fetch data
date_series = pd.concat(
    [
        df["order_date"],
        df["expected_delivery"],
        df["actual_delivery"]
    ]
).dropna()

#fetch MIN, MAX dates from data
start_date = date_series.min().normalize()
end_date = date_series.max().normalize()

#generate every date in-between min, max dates
date_range = pd.date_range(
    start=start_date,
    end=end_date,
    freq="D" #D = day basis
)

dim_date = pd.DataFrame(
    {
        "date": date_range
    }
)

dim_date["date_id"] = (
    dim_date["date"]
    .dt.strftime("%Y%m%d")
    .astype(int)
)

dim_date["year"] = (
    dim_date["date"]
    .dt.year
)

dim_date["quarter"] = (
    "Q" + dim_date["date"]
    .dt.quarter
    .astype(str)
)

dim_date["month"] = (
    dim_date["date"]
    .dt.month
)

dim_date["month_name"] = (
    dim_date["date"]
    .dt.month_name()
)

dim_date["week"] = (
    dim_date["date"]
    .dt.isocalendar()
    .week
    .astype(int)
)

dim_date["day"] = (
    dim_date["date"]
    .dt.day
)

dim_date["day_name"] = (
    dim_date["date"]
    .dt.day_name()
)

dim_date = dim_date[
    [
        "date_id",
        "date",
        "year",
        "quarter",
        "month",
        "month_name",
        "week",
        "day",
        "day_name"
    ]
]

#WRITE dimensions data to s3
#reusuable function:
def write_parquet(df, bucket, key):
    buffer = io.BytesIO()
    
    df.to_parquet(
        buffer,
        index = False
    )
    
    s3.put_object(
        Bucket = bucket,
        Key = key,
        Body = buffer.getvalue()
    )
    
    print(
        f"Written to: s3://{bucket}/{key}"
    )

write_parquet(
    dim_supplier,
    bucket, #check top of the codebase for reference
    "gold/dimensions/dim_supplier/dim_supplier.parquet"
)

write_parquet(
    dim_warehouse,
    bucket,
    "gold/dimensions/dim_warehouse/dim_warehouse.parquet"
)

write_parquet(
    dim_product,
    bucket,
    "gold/dimensions/dim_product/dim_product.parquet"
)

write_parquet(
    dim_date,
    bucket,
    "gold/dimensions/dim_date/dim_date.parquet"
)

#Data validation
print(
    f"Suppliers: {len(dim_supplier)}"
)

print(
    f"Products: {len(dim_product)}"
)

print(
    f"Warehouses: {len(dim_warehouse)}"
)

print(
    f"Dates: {len(dim_date)}"
)

#Uniqueness validation (if any data is duplicatedly present, the job will fail)
assert dim_supplier["supplier_id"].is_unique

assert dim_product["product_id"].is_unique

assert dim_warehouse["warehouse_id"].is_unique

assert dim_date["date_id"].is_unique

