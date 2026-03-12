import os
import sys
from google.cloud import bigquery
from google.cloud import language_v1
import logging

logging.basicConfig(
    level=logging.INFO
    , format= "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

def insert_empty_sentiments():
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_name = os.getenv("BQ_DATASET")
    table_name = os.getenv("BQ_TABLE_NEWS")
    table_path = f"{project_id}.{dataset_name}.{table_name}"
    location = os.getenv("GCP_PROJECT_LOC")
    
    logger.info(f"Env vars: {project_id}, {dataset_name}, {table_name}, {location}, {table_path}")
    
    try:
        bq_clinent = bigquery.Client(project = project_id, location = location)
        nl_client = language_v1.LanguageServiceClient()
        logging.info("Clients made.")
    except Exception as e:
        logging.error(f"ERROR making clients: {e}", exc_info=True)
        return []
    
    # fetching data from BQ table
    query = f"""
        Select 
            link, title, description
        From 
            `{table_path}`
        Where 
            sentiment_score is null
    """
    
    try: 
        records = list(bq_clinent.query(query))
        logger.info("Records fetched.")
    except Exception as e:
        logger.error(f"ERROR Fetching data from BQ: {e}", exc_info=True)
        return 0
    
    if not records:
        logger.info("No new records to process")
        return 0
    
    logger.info(f"there is {len(records)} new records")
    
    r_count = 0
    for row in records:
        full_text =  f"{row.title}. {row.description}"
        document = language_v1.Document(content = full_text, type = language_v1.Document.Type.PLAIN_TEXT)
        
        try:
            analysis = nl_client.analyze_sentiment(request = {"document": document})
            score =  round(analysis.document_sentiment.score, 1)
            logger.info(f"{row.title}. Score: {score}")
        except Exception as e:
            logger.error(f"ERROR while assessing score {e}")
            return 0
        
        query_up = f""" 
            Update `{table_path}`
            SET sentiment_score = @score
            Where link = @link
        """
        
        try:
            job_config = bigquery.QueryJobConfig(
                query_parameters = [
                    bigquery.ScalarQueryParameter("score", "FLOAT64", score),
                    bigquery.ScalarQueryParameter("link", "STRING", row.link)
                ]
            )
            bq_clinent.query(query_up, job_config = job_config).result()
            r_count += 1
            logger.info(f"Rows upadeted: {r_count}.")
            
        except Exception as e:
            logger.error(f"Error occured while updating sentiment: {e}")
    
    logger.info(f"All updated rows: {r_count}.")

if __name__ == "__main__":
    # data = insert_empty_sentiments()
    # for d in data:
    #     print(d)        
    insert_empty_sentiments()