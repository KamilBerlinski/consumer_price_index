from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os


project_id = os.getenv("GCP_PROJECT_ID")
dataset_id = os.getenv("BQ_DATASET")
location =   os.getenv("GCP_PROJECT_LOC")

def bigquery_upload(records):
    table_name = os.getenv("BQ_TABLE")
    client = bigquery.Client(project=project_id, location=location)
    table_path = f"{project_id}.{dataset_id}.{table_name}"

    try:
        client.get_table(table_path)
    except NotFound: 
        print(f"Table {table_name} not found in dataset: {dataset_id} in project: {project_id}.")

        schema = [
            bigquery.SchemaField("date", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("category", "STRING", mode="REQUIRED"), 
            bigquery.SchemaField("insert_date", "DATETIME", mode="REQUIRED"),
        ]

        table = bigquery.Table(table_path, schema=schema)
        client.create_table(table=table)
        print(f"Table {table_name} created in dataset: {dataset_id} in project: {project_id}.")
    
    if not records:
        print("No data.")
        return
    

    insert = client.insert_rows_json(table_path, records)

    if insert == []:
        print(f"Data sent. {len(records)} row/-s added.")
    else:
        print(f"Errors while sending: {insert}.")
        
    return insert


def bigquery_deduplicate_and_upload(news):
    table_name= os.getenv("BQ_TABLE_NEWS")
    client = bigquery.Client(project=project_id, location=location)
    table_path: str = f"{project_id}.{dataset_id}.{table_name}"

    query = f"""
        SELECT link
        FROM `{table_path}`
        WHERE published_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
    """
    data = client.query(query)
    existing_link = {row.link for row in data}
    unique_news = [n for n in news if n['link'] not in existing_link]

    if unique_news:
        articles = client.insert_rows_json(table_path, unique_news)
        if not articles:
            print(f"Added new {len(unique_news)} articles.")
        else:
            print(f"Errors: {articles}")
    else: 
        print("Mo new articles.")



