# IMPORTS
import sys
import boto3
import pandas as pd
import io
import uuid

from datetime import datetime
from awsglue.utils import getResolvedOptions

# FETCH Bucket and Key values coming from lambda function
args = getResolvedOptions(
    sys.argv,
    ['bucket','key']
)

bucket = args['bucket']
key = args['key']

print("Bucket:", bucket)
print("Key:", key)

# USE boto3 to allow python to connect with AWS services.
s3 = boto3.client("s3")

obj = s3.get_object(
    Bucket = bucket,
    Key = key
)

# READ EXCEL FILE
df = pd.read_excel(
    io.BytesIO(obj["Body"].read())
)

# print(df.head())

# STANDARDIZE COLUMN NAMES
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)

# print(df.columns.tolist())

# REMOVE leading and trailing spaces and STANDARDIZING capitalization of columnn data
text_columns = [
    "supplier_name",
    "warehouse_city",
    "product_name",
    "category",
    "carrier",
    "order_status",
    "payment_terms",
    "buyer_name",
    "priority",
    "inspection_status",
    "remarks",
    "transport_mode"
]

for col in text_columns:
    df[col] = (
        df[col]
        .astype(str)
        .str.strip()
        .str.title()
    )

# NORMALIZE Supplier, Warehouse, Transport Mode names
supplier_mapping = {
    "ABC PVT LTD": "ABC Pvt Ltd",
    "ABC PRIVATE LIMITED": "ABC Pvt Ltd",
    "ABC Pvt. Ltd.": "ABC Pvt Ltd",

    "DELL INDIA": "Dell India",

    "SAMSUNG INDIA": "Samsung India"
}

warehouse_mapping = {
    "Mumbai Wh": "Mumbai",
    "Mumbai Warehouse": "Mumbai",

    "Pune Wh": "Pune",
    "Pune Warehouse": "Pune",

    "Delhi Warehouse": "Delhi"
}

transport_mapping = {
    "Road": "Road",
    "Road Transport": "Road",
    "Truck": "Road",
    "By Road": "Road",

    "Air": "Air",

    "Sea": "Sea",

    "Rail": "Rail"
}

df["supplier_name"] = df["supplier_name"].replace(supplier_mapping)
df["warehouse_city"] = df["warehouse_city"].replace(warehouse_mapping)
df["transport_mode"] = df["transport_mode"].replace(transport_mapping)

# PARSE DATES
date_columns = [
    "order_date",
    "expected_delivery",
    "actual_delivery"
]

for col in date_columns:
    df[col] = pd.to_datetime(
        df[col],
        errors = "coerce" #coerce writes invalid date as NaT. Prevents crashing of the job.
    )

# FILL MISSING VALUES
df["shipping_cost"] = df["shipping_cost"].fillna(0)
df["carrier"] = df["carrier"].fillna("Unknown")
df["remarks"] = df["remarks"].fillna("No Remarks")
df["inspection_status"] = df["inspection_status"].fillna("Pending")

# REMOVE DUPLICATES
rows_before = len(df)

df = df.drop_duplicates()

rows_after = len(df)

duplicate_rows_removed = rows_after - rows_before

print(f"Duplicate rows removed: {duplicate_rows_removed}")

print("Starting data validation checks...")
# Creating new columns those make data make-sense from business logic perspective
def validate_quantity():
    df["quantity_issue"] = df["quantity_received"] > df["quantity_ordered"] # quantity received more than quanitity ordered
    
def validate_costs():
    df["invalid_unit_cost"] = df["unit_cost"] <= 0
    df["invalid_shipping_cost"] = df["shipping_cost"] < 0
    
def validate_dates():
    df["date_issue"] = df["actual_delivery"] < df["order_date"]
    df["is_delayed"] = df["actual_delivery"] > df["expected_delivery"]
    
def validate_missing_names():
    df["supplier_missing"] = df["supplier_name"].isna()
    df["warehouse_missing"] = df["warehouse_city"].isna()
    df["product_missing"] = df["product_name"].isna()

def validate_inspection():
    df["inspection_failed"] = df["inspection_status"] == "Rejected"

def validate_defect():
    df["defect_mismatch"] = (
        (df["inspection_status"] == "Rejection") 
        & 
        (df["defect_count"] == 0)
    )
    
# Overall data quality score
# Creating a flag column
def validate_quality_issue():
    validation_columns = [
        "quantity_issue",
        "invalid_unit_cost",
        "invalid_shipping_cost",
        "date_issue",
        "supplier_missing",
        "product_missing",
        "is_delayed",
        "inspection_failed",
        "defect_mismatch"
    ]

    df["has_quality_issue"] = df[validation_columns].any(axis = 1)

validate_quantity()
validate_costs()
validate_dates()
validate_missing_names()
validate_defect()
validate_inspection()
validate_quality_issue()

# DATA ENRICHMENT COLUMNS
etl_batch_id = str(uuid.uuid4())

df["etl_batch_id"] = etl_batch_id
df["etl_processed_timestamp"] = datetime.now()
df["delivery_delay_days"] = (
    df["actual_delivery"] - df["expected_delivery"]
).dt.days

df["lead_time_days"] = (
    df["actual_delivery"] - df["order_date"]
).dt.days

df["procurement_cost"] = (
    df["quantity_received"] * df["unit_cost"]
)

df["total_cost"] = (
    df["procurement_cost"] + df["shipping_cost"]
)



# OUTPUT PATH to save df:
output_key = "silver/supply_chain/supplyChain_Cleaned.parquet"

buffer = io.BytesIO()

#CONVERT File into parquet format to save in Silver layer
df.to_parquet(
    buffer,
    index = False
)

s3.put_object(
    Bucket = bucket,
    # Key = "silver/supply_chain/supplyChain_Cleaned.parquet",
    Key = output_key,
    Body = buffer.getvalue()
)








