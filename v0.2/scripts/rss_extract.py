import requests
from bs4 import BeautifulSoup
from datetime import datetime, UTC
from dateutil import parser
import pprint

rss_configs = [
    {
        "market": "ww", 
        "name": "CoinDesk", 
        "url": "https://www.coindesk.com/arc/outboundfeeds/rss/"
    },{
        "market": "pl", 
        "name": "Money.pl", 
        "url": "https://www.money.pl/rss/main.xml"
    }]


def fetch_market_news():
    all_news = []
    for page in rss_configs:
        print(f"Scraping  {page['name']} ( {page['market']})...")
        response = requests.get(page['url'])
        soup = BeautifulSoup(response.content, "xml")
        
        for item in soup.find_all("item")[:5]:
            raw_desc = item.find("description").text.strip()
            clean_desc = BeautifulSoup(raw_desc, "html.parser").get_text(strip=True)
            
            all_news.append({
                "market_source": page['market'],
                "source_name": page['name'],
                "title": item.find("title").text.strip(),
                "description": clean_desc,
                "link": item.find("link").text.strip(),
                "published_at": parser.parse(item.find("pubDate").text.strip()).isoformat(),
                "ingested_at": datetime.now(tz=UTC).isoformat()
            })
    return all_news


if __name__ == "__main__":
    data = fetch_market_news()
    pprint.pprint(data)

    # for d in data:
    #     print(d)
    #     print("-" * 30)