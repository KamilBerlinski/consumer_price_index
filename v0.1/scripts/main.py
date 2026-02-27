from flask import Flask, jsonify
import sys
import traceback
from utils import bigquery_upload
from indexes_extract import fetch_gpw
from metals_extract import fetch_metals_data
from fx_rates_extarct import fetch_fx_rates

app = Flask(__name__)

@app.route('/run-indexes', methods=['GET'])
def run_indexes():
    try:
        print("DEBUG: Fetching indexes data...", file=sys.stderr)
        data = fetch_gpw()
        print(f"DEBUG: Records dowloaded: {len(data)}. Uploading to BQ...", file=sys.stderr)
        
        bigquery_upload(data)
        
        return jsonify({"status": "success", "count": len(data)}), 200

    except Exception as e:
        print("CRITICAL ERROR in  main.py:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        
        return jsonify({"status": "error", "message": str(e)}), 500
    

@app.route('/run-metals', methods=['GET'])
def run_metals():
    try:
        print("DEBUG: Fetching metal data...", file=sys.stderr)
        data = fetch_metals_data()
        print(f"DEBUG: Records dowloaded: {len(data)}. Uploading to BQ...", file=sys.stderr)
        
        bigquery_upload(data)
        return jsonify({"status": "success", "count": len(data)}), 200

    except Exception as e:
        print("CRITICAL ERROR in  main.py:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/run-fx-rates", methods=["GET"])
def run_fx_rates():
    try:
        print("DEBUG: Fetching FX rates data...", file=sys.stderr)
        data = fetch_fx_rates()
        print(f"DEBUG: Records dowloaded: {len(data)}. Uploading to BQ...", file=sys.stderr)
        
        bigquery_upload(data)
        return jsonify({"status": "success", "count": len(data)}), 200
        
    except Exception as e:
        print("CRITICAL ERROR in  main.py:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)