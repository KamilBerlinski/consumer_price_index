
import feedparser

feed = feedparser.parse("https://www.coindesk.com/arc/outboundfeeds/rss/")
item = feed.entries[0]

for key in item.keys():
    wartosc = str(item.get(key))
    print(f"{key}: {wartosc[:300]}...")