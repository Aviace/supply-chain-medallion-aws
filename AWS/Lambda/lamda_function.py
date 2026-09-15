import json
import boto3

glue = boto3.client("glue")

GLUE_JOB_NAME = "SupplyChain-Silver-ETL"

def lambda_handler(event, context):

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--bucket":bucket,
            "--key":key
        }

    )

    print(f"Glue Job Started for {key}")

    return {
        "statusCode": 200,
        "body": json.dumps(response)
    }