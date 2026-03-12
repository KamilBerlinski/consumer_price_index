from datetime import datetime, timezone
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup as bs

def fetch_nbp_rates():
    url: str = "https://static.nbp.pl/dane/stopy/stopy_procentowe.xml"
    page = requests.get(url)
    soup = bs(page.content, "xml")
    ref_tag = soup.find("pozycja", {"id": "ref"})

    if ref_tag:
        price = ref_tag["oprocentowanie"].replace(",", ".")
        eff_date = ref_tag["obowiazuje_od"]
        record = {
            "date": f"{eff_date} 00:00:00" ,
            "symbol": "REFFERENCE_RATE",
            "price": round(float(price), 3),
            "category": "interest_rate", 
            "insert_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:00")
        }
        return [record]
    return []
    
    
# if __name__ == "__main__":
#     data = fetch_nbp_rates()
#     print(data)