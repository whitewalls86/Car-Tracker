import os
import sqlite3
from datetime import date
from config import DB_PATH
from contextlib import contextmanager
from typing import Optional, Generator, List, Dict, Tuple


@contextmanager
def get_db_conn(existing_conn: Optional[sqlite3.Connection] = None) -> Generator[sqlite3.Connection, None, None]:
    if existing_conn:
        yield existing_conn
    else:
        conn = sqlite3.connect(DB_PATH)
        try:
            yield conn
        finally:
            conn.close()


def init_db():
    os.makedirs("data", exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        # Main listings table using VIN as the unique identifier
        cur.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            vin TEXT PRIMARY KEY,
            listing_id TEXT,
            title TEXT,
            price INTEGER,
            msrp INTEGER,
            mileage INTEGER,
            dealer TEXT,
            location TEXT,
            distance INTEGER,
            shipping_cost REAL,
            search_scope TEXT,
            url TEXT,
            image_url TEXT,
            days_on_market INTEGER,
            date_added DATE,
            first_seen DATE,
            last_seen DATE,
            status TEXT DEFAULT 'active'
        )
        """)

        # Price tracking history
        cur.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vin TEXT,
            date DATE,
            price INTEGER,
            FOREIGN KEY (vin) REFERENCES listings(vin)
        )
        """)

        conn.commit()


def get_vins_by_listing_ids(listing_ids: List[str]) -> dict:
    placeholders = ','.join('?' for _ in listing_ids)
    query = f"SELECT listing_id, vin FROM listings WHERE listing_id IN ({placeholders})"

    with get_db_conn(existing_conn=None) as conn:
        cur = conn.cursor()
        cur.execute(query, listing_ids)
        rows = cur.fetchall()

    return {listing_id: vin for listing_id, vin in rows}


def flush_listings_to_db(listings: List[Dict]) -> None:
    if not listings:
        return

    insert_values = [
        (
            listing['vin'], listing['listing_id'], listing['price'], listing.get('title'), listing.get('mileage'),
            listing.get('dealer'), listing.get('location'), listing.get('distance'), listing.get('shipping_cost'),
            listing.get('search_scope'), listing.get('url'), listing.get('image_url'),
            listing.get('days_on_market'), listing.get('date_added'), listing.get('msrp')
        ) for listing in listings if 'vin' in listing and listing['vin']
    ]

    price_log_candidates = [
        (listing['vin'], listing['price']) for listing in listings
        if listing.get('vin') and listing.get('price') is not None
    ]

    with get_db_conn(existing_conn=None) as conn:
        cur = conn.cursor()

        # Insert or update listings
        cur.executemany("""
            INSERT INTO listings (
                vin, listing_id, title, price, mileage, dealer, location, distance,
                shipping_cost, search_scope, url, image_url, days_on_market, date_added, msrp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(vin) DO UPDATE SET
                price = excluded.price,
                last_seen = CURRENT_DATE
        """, insert_values)

        # Batch deduplication for price history
        if price_log_candidates:
            vin_set = {vin for vin, _ in price_log_candidates}
            placeholders = ','.join('?' for _ in vin_set)
            cur.execute(
                f"SELECT vin FROM price_history WHERE date = ? AND vin IN ({placeholders})",
                (date.today(), *vin_set)
            )
            existing_vins = {row[0] for row in cur.fetchall()}

            new_price_logs = [
                (vin, date.today(), price)
                for vin, price in price_log_candidates
                if vin not in existing_vins
            ]

            if new_price_logs:
                cur.executemany(
                    "INSERT INTO price_history (vin, date, price) VALUES (?, ?, ?)",
                    new_price_logs
                )

        conn.commit()


def get_all_active_listing_ids(today: date = date.today()) -> List[Tuple[str, str]]:
    query = """
        SELECT vin, url FROM listings
        WHERE status = 'active' AND last_seen < ?
    """
    with get_db_conn(existing_conn=None) as conn:
        cur = conn.cursor()
        cur.execute(query, (today,))
        return cur.fetchall()


def log_price(vin: str, price: int, conn: Optional[sqlite3.Connection] = None) -> None:
    with get_db_conn(conn) as db:
        cur = db.cursor()
        cur.execute("SELECT 1 FROM price_history WHERE vin = ? AND date = ?", (vin, date.today()))
        if cur.fetchone() is None:
            cur.execute("INSERT INTO price_history (vin, date, price) VALUES (?, ?, ?)", (vin, date.today(), price))
            db.commit()


def refresh_cleaned_listings(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS cleaned_listings;")

    cur.executescript("""
    CREATE TABLE cleaned_listings AS
    WITH clean_data AS (
        SELECT
            CASE 
                WHEN title LIKE '%2025%' THEN '2025'
                WHEN title LIKE '%2024%' THEN '2024'
                WHEN title LIKE '%2023%' THEN '2023'
                WHEN title LIKE '%2026%' THEN '2026'
                WHEN title LIKE '%2022%' THEN '2022'
                WHEN title LIKE '%2021%' THEN '2021'
                WHEN title LIKE '%2020%' THEN '2020'
                WHEN title LIKE '%2019%' THEN '2019'
            END as year,
            CASE
                WHEN title LIKE '%Volkswagen%' THEN 'Volkswagen'
                WHEN title LIKE '%Honda%' THEN 'Honda'
                WHEN title LIKE '%Kia%' THEN 'Kia'
                WHEN title LIKE '%Subaru%' THEN 'Subaru'
                WHEN title LIKE '%Tucson%' THEN 'Hyundai'
                WHEN title LIKE '%Mazda%' THEN 'Mazda'
                WHEN title LIKE '%Toyota%' THEN 'Toyota'
                WHEN title LIKE '%Santa Fe%' THEN 'Hyundai'
                WHEN title LIKE '%Escape%' THEN 'Ford'
                ELSE title
            END AS make,
            CASE
                WHEN title LIKE '%Volkswagen%' THEN 'Tiguan'
                WHEN title LIKE '%Honda%' THEN 'CRV'
                WHEN title LIKE '%Kia%' THEN 'Sportage'
                WHEN title LIKE '%Subaru%' THEN 'Forester'
                WHEN title LIKE '%Tucson%' THEN 'Tucson'
                WHEN title LIKE '%Mazda%' THEN 'cx-50'
                WHEN title LIKE '%Toyota%' THEN 'Rav4'
                WHEN title LIKE '%Santa Fe%' THEN 'Santa Fe'
                WHEN title LIKE '%Escape%' THEN
                    CASE WHEN title LIKE '%PHEV%' THEN 'Escape-PHEV'
                         ELSE 'Escape'
                    END
                ELSE title
            END AS model,
            CASE
                WHEN title LIKE '%Volkswagen%' THEN
                    CASE
                        WHEN title LIKE '%Wolfs%' THEN 'Wolfsburg'
                        WHEN title LIKE '%R-Line%' THEN 'R-Line'
                        WHEN title LIKE '% SE%' THEN 'SE'
                        WHEN title LIKE '% SEL%' THEN 'SEL'
                        WHEN title LIKE '% S%' THEN 'S'
                    END
                WHEN title LIKE '%Honda%' THEN
                    CASE
                        WHEN title LIKE '%Touring%' THEN 'Touring'
                        WHEN title LIKE '%-L%' THEN 'Sport L'
                        WHEN title LIKE '%Sport%' THEN 'Sport'
                    END
                WHEN title LIKE '%Kia%' THEN
                    CASE
                        WHEN title LIKE '%LX%' THEN 'LX'
                        WHEN title LIKE '%EX%' THEN 'EX'
                        WHEN title LIKE '%SX%' THEN 'SX'
                    END
                WHEN title LIKE '%Subaru%' THEN
                    CASE
                        WHEN title LIKE '%Limited%' THEN 'Limited'
                        WHEN title LIKE '%Premium%' THEN 'Premium'
                        WHEN title LIKE '%Sport%' THEN 'Sport'
                        WHEN title LIKE '%Touring%' THEN 'Touring'
                        Else 'N/A'
                    END
                WHEN title LIKE '%Tucson%' THEN
                    CASE
                        WHEN title LIKE '%Line%' THEN 'N-Line'
                        WHEN title LIKE '%Blue%' THEN 'Blue'
                        WHEN title LIKE '%SEL%' THEN 'SEL'
                        WHEN title LIKE '%Limit%' THEN 'Limited'
                    END
                WHEN title LIKE '%Mazda%' THEN
                    CASE
                        WHEN title LIKE '%Plus%' THEN 'Prem Plus'
                        WHEN title LIKE '%Premium%' THEN 'Prem'
                        WHEN title LIKE '%Preferred%' THEN 'Pref'
                    END
                WHEN title LIKE '%Toyota%' THEN
                    CASE
                        WHEN title LIKE '%Wood%' THEN 'Woodland'
                        WHEN title LIKE '%SE%' THEN 'SE'
                        WHEN title LIKE '%XLE%' THEN 'XLE'
                        WHEN title LIKE '%LE%' THEN 'LE'
                        WHEN title LIKE '%Limit%' THEN 'Limited'
                        ELSE 'N/A'
                    END
                WHEN title LIKE '%Santa Fe%' THEN 'Santa Fe'
                WHEN title LIKE '%Escape%' THEN
                    CASE 
                        WHEN title LIKE '%PHEV%' THEN
                            CASE 
                                WHEN title LIKE '%SE%' and title not like '%base%' THEN 'SE'
                                ELSE 'base'
                            END
                        ELSE
                            CASE
                                WHEN title like '%Platinum%' THEN 'Platinum'
                                WHEN title like '%Titanium%' THEN 'Titanium'
                                WHEN title like '%Active%' THEN 'Active'
                                WHEN title like '%SE%' THEN 'SE'
                                WHEN title like '%Line%' THEN 'ST-Line'
                            END
                    END
                ELSE title
            END as trim,
            CASE
                WHEN days_on_market IS NULL THEN
                    CAST(julianday(last_seen) - julianday(first_seen) AS INTEGER)
                ELSE NULL
            END AS implied_days_on_market,
            COALESCE(days_on_market, CAST(julianday(last_seen) - julianday(first_seen) AS INTEGER)) as 
            calculated_days_on_market,
            *
        FROM listings
    ),
    avg_msrp AS (
        SELECT year, model, trim, AVG(msrp) AS avg_msrp
        FROM clean_data
        WHERE msrp IS NOT NULL
        GROUP BY year, model, trim
    ),
    enriched AS (
        SELECT
            cd.*,
            am.avg_msrp,
            COALESCE(cd.msrp, am.avg_msrp) AS implied_msrp
        FROM clean_data cd
        LEFT JOIN avg_msrp am
            ON cd.year = am.year AND cd.model = am.model AND cd.trim = am.trim
    ),
    more_encriched as (
        SELECT 
            en.*
            ,CASE 
              WHEN price IS NOT NULL THEN price
              WHEN implied_msrp IS NOT NULL THEN implied_msrp
              WHEN msrp IS NOT NULL THEN msrp
              ELSE NULL
            END AS implied_price
        FROM enriched en
    )
    SELECT 
        *
        ,implied_msrp - COALESCE(price, implied_price) AS discount
        ,(implied_msrp - COALESCE(price, implied_price)*1.0) / implied_msrp AS discount_rate
    FROM more_encriched
    """)

    conn.commit()
    conn.close()
    print(" cleaned_listings table refreshed.")
