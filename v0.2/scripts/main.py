from flask import Flask, jsonify
from utils import bigquery_upload, bigquery_deduplicate_and_upload
from indexes_extract import fetch_gpw
from metals_extract import fetch_metals_data
from fx_rates_extarct import fetch_fx_rates
from oil_gas_extract import fetch_oil_gas
from interest_rates_extract import fetch_nbp_rates
from rss_extract import fetch_market_news
from sentiment_updater import insert_empty_sentiments
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/run-indexes', methods=['GET'])
def run_indexes():
    try:
        logger.info("Fetching indexes data...")
        data = fetch_gpw()
        logger.info(f"Records dowloaded: {len(data)}. Uploading to BQ...")
        bigquery_upload(data)        
        return jsonify({"status": "success", "count": len(data)}), 200

    except Exception as e:
        logger.error("ERROR in  main.py in /run-indexes: ", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500
    

@app.route('/run-metals', methods=['GET'])
def run_metals():
    try:
        logger.info("Fetching metal data...")
        data = fetch_metals_data()
        logger.info(f"Records dowloaded: {len(data)}. Uploading to BQ...")
        bigquery_upload(data)
        return jsonify({"status": "success", "count": len(data)}), 200

    except Exception as e:
        logger.error("ERROR in main.py in /run-metals: ", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/run-fx-rates", methods=["GET"])
def run_fx_rates():
    try:
        logger.info("Fetching FX rates data...")
        data = fetch_fx_rates()
        logger.info(f"Records dowloaded: {len(data)}. Uploading to BQ...")
        bigquery_upload(data)
        return jsonify({"status": "success", "count": len(data)}), 200
        
    except Exception as e:
        logger.error("ERROR in main.py in /run-fx-rates: ", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/run-news", methods=["GET"])
def run_news():
    try:
        logger.info("Fetching News data...", exc_info=True)
        data = fetch_market_news()
        logger.info(f"Records dowloaded: {len(data)}.", exc_info=True)  
        logger.info("Deduplicating and uploading...", exc_info=True)
        bigquery_deduplicate_and_upload(data)
        return jsonify({"status": "success", "count": len(data)}), 200     

    except Exception as e:
        logger.error(f"ERROR in main.py in /run-news: ", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500        


@app.route("/run-oil-gas", methods=["GET"])
def run_oil_gas():
    try:
        logger.info("Fetching Oil&Gas data...", exc_info=True)
        data = fetch_oil_gas()
        logger.info(f"Records dowloaded: {len(data)}. Uploading to BQ...", exc_info=True)  
        bigquery_upload(data)
        return jsonify({"status": "success", "count": len(data)}), 200     

    except Exception as e:
        logger.error(f"ERROR in main.py in /run-oil-gas: ", exc_info=True)   
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/run-nbp-rates', methods=['GET'])
def run_nbp():
    try:
        logger.info("Fetching NBP data...")
        data = fetch_nbp_rates()
        logger.info(f"Records dowloaded: {len(data)}. Uploading to BQ...")
        bigquery_upload(data)
        return jsonify({"status": "success", "count": len(data)}), 200

    except Exception as e:
        logger.error("ERROR in  main.py:", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500       
    
    
@app.route("/update-sentiment", methods=['GET'])
def update_sentiments():
    try:
        logger.info("Fetchnig data from BQ and adding sentiment scorring..")
        data = insert_empty_sentiments()
        logger.info(f"{data} rows analysied.")
        return jsonify({"status": "success", "updated_records": data}), 200

    except Exception as e:
        logger.error("Error in main.py in /update-sentiment: ", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500
    
    
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)