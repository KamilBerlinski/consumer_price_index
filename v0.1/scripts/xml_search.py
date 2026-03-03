import requests
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin

def find_xml_links(url):
    # KROK 1: Udajemy przeglądarkę
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    found_xmls = set() 
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = bs(response.content, "html.parser")
        

        for a in soup.find_all('a', href=True):
            if '.xml' in a['href'].lower():
                found_xmls.add(urljoin(url, a['href']))
        
        for tag in soup.find_all(True): 
            for attr_name, attr_value in tag.attrs.items():
                if isinstance(attr_value, str) and '.xml' in attr_value.lower():
                    found_xmls.add(urljoin(url, attr_value))
                    
        return list(found_xmls)
        
    except Exception as e:
        print(f"Error while searching page: {e}")
        return []

if __name__ == "__main__":
    url = "https://nbp.pl/polityka-pieniezna/decyzje-rpp/podstawowe-stopy-procentowe-nbp/"
    links = find_xml_links(url)
    
    print(f"Found {len(links)} XML sources:")
    for link in links:
        print(f"-> {link}")