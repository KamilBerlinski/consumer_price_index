import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
from utils import bigquery_upload

def fetch_metals_data():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:00") 
    records = []

    assets = {
        "GC=F": "metal", 
        "PL=F": "metal",
        "PA=F": "metal",
        "HG=F": "metal",
        "SI=F": "metal"
    }

    for ticker, cat in assets.items():
        try:
            data = yf.Ticker(ticker)
            dates_r = data.history(period="5d")

            if not dates_r.empty:
                ref_date = dates_r.index[-1].strftime("%Y-%m-%d %H:%M:00")
                price = dates_r['Close'].iloc[-1]
                records.append({
                    "date": ref_date,
                    "symbol": ticker,
                    "price": round(float(price), 3),
                    "category": cat, 
                    "insert_date": now,
                })
            else:
                print(f"No data for {ticker}.")
        except Exception as e:
            print(f"Dowloading error {ticker}: {e}")

    return records


# if __name__ == "__main__":
#     data = fetch_metals_data()
#     for i in data:
#         print(i)
