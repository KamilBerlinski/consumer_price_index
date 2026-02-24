from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os

def bigquery_upload(records):
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET")
    table_name = os.getenv("BQ_TABLE")

    client = bigquery.Client(project=project_id, location="EU")
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    try:
        client.get_table(table_id)
    except NotFound: 
        print(f"Table {table_name} not found in dataset: {dataset_id} in project: {project_id}.")

        schema = [
            bigquery.SchemaField("date", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("category", "STRING", mode="REQUIRED"), 
            bigquery.SchemaField("insert_date", "DATETIME", mode="REQUIRED"),
        ]

        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table=table)
        print(f"Table {table_name} created in dataset: {dataset_id} in project: {project_id}.")
    
    if not records:
        print("No data.")
        return
    

    errors = client.insert_rows_json(table_id, records)

    if errors == []:
        print(f"Data sent. {len(records)} row/-s added.")
    else:
        print(f"Errors while sending: {errors}.")
        
    return errors