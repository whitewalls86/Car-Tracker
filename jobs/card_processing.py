from typing import Dict
from job import Job, PrioritizedJobQueue, SharedState
from db import flush_listings_to_db
from utils.soup_helpers import extract_price
from bs4 import Tag
from utils.job_utils import enqueue_with_priority


class SaveJob(Job):
    """
    Adds a processed listing to the shared buffer. Triggers a flush job if threshold is reached.
    """
    def __init__(self, listing: Dict, shared_state: SharedState):
        self.listing = listing
        self.shared_state = shared_state

    def run(self, job_queue: PrioritizedJobQueue) -> None:
        tracker = self.shared_state.tracker
        tracker.record_start(self.__class__.__name__)
        should_flush = self.shared_state.listing_buffer.add(self.listing)
        if should_flush:
            enqueue_with_priority(job_queue, FlushSaveBufferJob(self.shared_state))

        tracker.record_complete(self.__class__.__name__)


class FlushSaveBufferJob(Job):
    """
    Writes the batched listings to the database using UPSERT logic.
    """
    def __init__(self, shared_state: SharedState):
        self.shared_state = shared_state

    def run(self, job_queue: PrioritizedJobQueue) -> None:
        tracker = self.shared_state.tracker
        tracker.record_start(self.__class__.__name__)
        listings = self.shared_state.listing_buffer.flush()
        flush_listings_to_db(listings)
        tracker.record_complete(self.__class__.__name__)


class DetailScrapeJob(Job):
    """
    Fetches the detail page for a new listing, extracts full data, and submits it to the DB buffer.
    """
    def __init__(self, listing_id: str, card: Tag, shared_state: SharedState):
        self.listing_id = listing_id
        self.card = card
        self.shared_state = shared_state

    def run(self, job_queue: PrioritizedJobQueue) -> None:
        from page_fetcher import fetch_soup_with_fallback
        from datetime import date, timedelta
        tracker = self.shared_state.tracker
        tracker.record_start(self.__class__.__name__)

        relative_url = self.card.select_one("a.image-gallery-link")['href']
        detail_url = f"https://www.cars.com{relative_url}"
        soup, req_type = fetch_soup_with_fallback(detail_url, 10)
        if not soup:
            print(f"[DetailScrapeJob] Failed to fetch detail for {self.listing_id}")
            return

        def get_text(selector):
            el = self.card.select_one(selector)
            return el.text.strip() if el else None

        def extract_vin_and_mileage(soup_obj):
            vin_val, mileage_val = None, None
            dt_tags = soup_obj.find_all("dt")
            for dt in dt_tags:
                if dt.text.strip().lower() == "vin":
                    dd = dt.find_next_sibling("dd")
                    vin_val = dd.text.strip() if dd else None
                elif dt.text.strip().lower() == "mileage":
                    dd = dt.find_next_sibling("dd")
                    if dd:
                        m = dd.text.strip().replace(" mi.", "").replace(",", "")
                        mileage_val = int(m) if m.isdigit() else None
            return vin_val, mileage_val

        vin, mileage = extract_vin_and_mileage(soup)
        days_tag = soup.select_one("div.price-history-summary div.listed-time strong")
        days_on_market = int(days_tag.text.strip()) if days_tag else None

        price = extract_price(soup)

        msrp = get_text("span.secondary-price")
        if msrp and "MSRP" in msrp:
            msrp = int(msrp.replace("MSRP", "").replace("$", "").replace(",", "").strip())
        else:
            msrp = None

        dealer = get_text("div.dealer-name strong")
        location = get_text("div.miles-from")
        title = get_text("h2.title")

        try:
            raw_distance = location.split("(")[-1].split("mi.")[0].strip().replace(",", "") if location else None
            distance = int(raw_distance) if raw_distance and raw_distance.isdigit() else None
        except (ValueError, AttributeError):
            distance = None

        img_tag = self.card.select_one("img.vehicle-image")
        image_url = img_tag['src'] if img_tag else None

        if distance:
            shipping_cost = round(distance * .75, 2)
        else:
            shipping_cost = None

        listing = {
            "vin": vin,
            "listing_id": self.listing_id,
            "title": title,
            "price": price,
            "mileage": mileage,
            "dealer": dealer,
            "location": location,
            "distance": distance,
            "shipping_cost": shipping_cost,
            "search_scope": None,
            "url": detail_url,
            "image_url": image_url,
            "days_on_market": days_on_market,
            "date_added": (date.today() - timedelta(days=days_on_market)) if days_on_market else None,
            "msrp": msrp
        }

        enqueue_with_priority(job_queue, SaveJob(listing, self.shared_state))
        tracker.record_complete(self.__class__.__name__)
