import yfinance as yf
from datetime import datetime, timezone

def fetch_oil_gas():
    records = []
    types = {
        "CL=F": "oil WTI"
        , "BZ=F": "oil Brent"
        , "NG=F": "gas"
    }   
    tickers = list(types.keys())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:00") 

    try: 
        data = yf.download(tickers, period="5d", interval="1d", group_by="ticker")
        
        for ticker in tickers:
            ticker_data = data[ticker]
            ticker_data = ticker_data.dropna(subset=["Close"])

            if not ticker_data.empty:
                ref_date = ticker_data.index[-1].strftime("%Y-%m-%d %H:%M:00") 
                price =  ticker_data["Close"].values[-1]
                records.append({
                    "date": ref_date,
                    "symbol": ticker,
                    "price": round(float(price), 3),
                    "category": types[ticker], 
                    "insert_date": now
                })
            else: 
                print(f"Error when processing data to table on {ticker}") 
    except Exception as e:
        print(f"Error: {e}")

    return records


# if __name__ == "__main__":
#     data = fetch_oil_gas()
#     print(data)

