import os

SEARCH_CONFIG = {
    # Base ZIP code for local search radius
    "zip": "77080",

    # Maximum distance (in miles) for local search
    "radius": 300,

    # Cost per mile for estimating vehicle shipping
    "shipping_cost_per_mile": 0.70,

    # Number of search result pages to scrape
    "pages": 40,

    # Number of listings per page
    "page_size": 100,

    # List of makes and models to track
    "models": [
        {"make": "honda", "model": "honda-cr_v_hybrid"},
        {"make": "toyota", "model": "toyota-rav4_hybrid"},
        {"make": "volkswagen", "model": "volkswagen-tiguan"},
        {"make": "hyundai", "model": "hyundai-tucson_hybrid"},
        {"make": "mazda", "model": "mazda-cx_50_hybrid"},
        {"make": "subaru", "model": "subaru-forester_hybrid"},
        {"make": "kia", "model": "kia-sportage_hybrid"},
        {"make": "ford", "model": "ford-escape_phev"},
        {"make": "ford", "model": "ford-escape"},
    ],

    # Priority: local listings are processed first; national listings skip VINs already seen locally
    "prioritize_local_first": True
}

JOB_PRIORITIES = {
    "PageLoadJob": 1,
    "ListingIDResolutionJob": 2,
    "DetailScrapeJob": 3,
    "VerifierJob": 4,
    "SaveJob": 5,
    "FlushSaveBufferJob": 6
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "cars.db")
PAGE_SIZE = 100
BASE_URL = "https://www.cars.com/shopping/results/"

ENQUEUE_BATCH_SIZE = 25
