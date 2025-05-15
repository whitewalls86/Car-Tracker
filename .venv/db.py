import os
import sqlite3
from datetime import date

def init_db():
    os.makedirs("data", exist_ok=True)
    with sqlite3.connect("data/cars.db") as conn:
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


def listing_exists_by_vin(vin):
    with sqlite3.connect("data/cars.db") as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM listings WHERE vin = ?", (vin,))
        return cur.fetchone() is not None


def listing_exists_by_listing_id(listing_id):
    with sqlite3.connect("data/cars.db") as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM listings WHERE listing_id = ?", (listing_id,))
        return cur.fetchone() is not None


def save_or_update_listing(data):
    with sqlite3.connect("data/cars.db") as conn:
        cur = conn.cursor()

        # Insert or ignore listing
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

        # Always update listing_id, price, last_seen, and search scope
        cur.execute("""
            UPDATE listings SET
                listing_id = ?,
                price = ?,
                last_seen = ?,
                search_scope = ?,
                distance = ?,
                shipping_cost = ?
            WHERE vin = ?
        """, (
            data["listing_id"], data["price"], date.today(), data["search_scope"],
            data["distance"], data["shipping_cost"], data["vin"]
        ))

        conn.commit()


def log_price(vin, price):
    with sqlite3.connect("data/cars.db") as conn:
        cur = conn.cursor()
        # Check if a price already exists today for this VIN
        cur.execute("""
            SELECT 1 FROM price_history WHERE vin = ? AND date = ?
        """, (vin, date.today()))
        if cur.fetchone() is None:
            cur.execute("""
                INSERT INTO price_history (vin, date, price)
                VALUES (?, ?, ?)
            """, (vin, date.today(), price))
            conn.commit()
