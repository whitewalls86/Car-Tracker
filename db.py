import os
import sqlite3
from datetime import date
from config import DB_PATH
from contextlib import contextmanager
from typing import Optional, Generator


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


def listing_exists(field: str, value: str) -> bool:
    assert field in {"vin", "listing_id"}, "Invalid field for existence check."
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT 1 FROM listings WHERE {field} = ?", (value,))
        return cur.fetchone() is not None


def listing_exists_by_vin(vin: str) -> bool:
    return listing_exists("vin", vin)


def listing_exists_by_listing_id(listing_id: str) -> bool:
    return listing_exists("listing_id", listing_id)


def get_vin_from_listing_id(listing_id: str) -> Optional[str]:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT vin FROM listings WHERE listing_id = ?", (listing_id,))
        row = cur.fetchone()
        return row[0] if row else None


def save_or_update_listing(data: dict, conn: Optional[sqlite3.Connection] = None) -> None:
    with get_db_conn(conn) as db:
        cur = db.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO listings (
                vin, listing_id, title, price, msrp, mileage, dealer, location, distance,
                shipping_cost, search_scope, url, image_url, days_on_market,
                date_added, first_seen, last_seen, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["vin"], data["listing_id"], data["title"], data["price"], data["msrp"], data["mileage"],
            data["dealer"], data["location"], data["distance"], data["shipping_cost"],
            data["search_scope"], data["url"], data["image_url"], data["days_on_market"],
            data["date_added"], date.today(), date.today(), "active"
        ))

        cur.execute("""
            UPDATE listings SET
                listing_id = ?,
                price = ?,
                last_seen = ?,
                search_scope = ?
            WHERE vin = ?
        """, (
            data["listing_id"], data["price"], date.today(), data["search_scope"], data["vin"]
        ))
        db.commit()


def log_price(vin: str, price: int, conn: Optional[sqlite3.Connection] = None) -> None:
    with get_db_conn(conn) as db:
        cur = db.cursor()
        cur.execute("SELECT 1 FROM price_history WHERE vin = ? AND date = ?", (vin, date.today()))
        if cur.fetchone() is None:
            cur.execute("INSERT INTO price_history (vin, date, price) VALUES (?, ?, ?)", (vin, date.today(), price))
            db.commit()


def update_and_log(data):
    conn = sqlite3.connect(DB_PATH)
    try:
        save_or_update_listing(data, conn)
        log_price(data["vin"], data["price"], conn)
        conn.commit()
    finally:
        conn.close()


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
