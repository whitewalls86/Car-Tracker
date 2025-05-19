import requests
from bs4 import BeautifulSoup
import fake_useragent

def save_and_beautify_listing_html(url, filename="listing_debug.html"):
    headers = {"User-Agent": fake_useragent.UserAgent().random}
    res = requests.get(url, headers=headers)

    if res.status_code == 200:
        soup = BeautifulSoup(res.text, "html.parser")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(soup.prettify())
        print(f"Saved beautified HTML to {filename}")
    else:
        print(f"Failed to fetch {url} — Status code: {res.status_code}")


save_and_beautify_listing_html(url="https://www.cars.com/vehicledetail/67bf0896-c314-4050-abb0-75f44f9c07d2/")