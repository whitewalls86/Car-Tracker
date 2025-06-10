def check_listing_still_active(soup) -> bool:
    """
    Returns True if the listing is still active.
    """
    return soup.select_one("spark-notification.unlisted-notification[open]") is None


def extract_price(soup) -> int | None:
    """
    Extracts the price from the soup, if present.
    """
    el = soup.select_one("span.primary-price")
    return int(el.text.strip().replace("$", "").replace(",", "")) if el and "$" in el.text else None
