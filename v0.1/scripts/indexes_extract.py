import pandas as pd
from datetime import datetime
import re
from utils import bigquery_upload

def fetch_gpw():
    url = "https://www.bankier.pl/gielda/notowania/indeksy-gpw"
    date_now = datetime.now().strftime("%Y-%m-%d %H:%M:00")
    records = []
    try:
        data = pd.concat(pd.read_html(url, decimal= ",", thousands= " ")[0:3], ignore_index=True)
        
        for _, row in data.iterrows():
            records.append({
                "date": datetime.strptime(row["Czas"], "%Y-%m-%d %H:%M").strftime("%Y-%m-%d %H:%M:00")
                , "symbol": row["Walor"]
                , "price": float(re.sub(r'\s+', '', row["Kurs"]).replace(',', '.'))
                , "category": "index_gpw"
                , "insert_date": date_now,
            })
            
    except Exception as e:
        print(e)
    
    return records


if __name__ == "__main__":
    data = fetch_gpw()
    # for d in data:
    #     print(d)
    if data:
        bigquery_upload(data)

