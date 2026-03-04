import requests
from datetime import datetime
from utils import bigquery_upload

def fetch_fx_rates():
    curr_list: list = {"USD", "EUR", "CHF", "GBP"}
    output = []
    date_now = datetime.today().strftime("%Y-%m-%d %H:%M:00")
    
    for cur in curr_list:
        try:
            url = f"https://api.nbp.pl/api/exchangerates/rates/a/{cur}/?format=json"
            response = requests.get(url=url, timeout=10)
            data = response.json()

            record = {
                    "date": data["rates"][0]["effectiveDate"] 
                    , "symbol": data["code"]
                    , "price": data["rates"][0]["mid"]
                    , "category": "FX_rates"
                    , "insert_date": date_now  
                }
            output.append(record)
            
        except Exception as e:
            print(f"Dowloading error {cur}. {e}")
            
    return output
    #print(f"{len(output)} records downloaded." )

# if __name__ == "__main__":
#     data = fetch_fx_rates()
#     for d in data:
#         print(d)
