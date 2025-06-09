import requests
import random
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent
from user_agent_tracking import get_valid_user_agents, log_user_agent, read_user_agent_set

total_bytes_downloaded = 0
total_requests_made = 0


def fetch_soup_with_fallback(url, max_attempts=10):
    global total_bytes_downloaded, total_requests_made
    user_agents = get_valid_user_agents()
    tried_user_agents = set()
    failed_user_agents = read_user_agent_set("failed_user_agents.log")

    for ua in random.sample(user_agents, min(max_attempts, len(user_agents))):
        soup = try_agent(url, ua)
        if soup is not None:
            return soup, "requests"

    # Try generating and testing new random user agents before cloudscraper
    ua_generator = UserAgent()
    for _ in range(max_attempts):
        ua = ua_generator.random
        if ua in tried_user_agents or ua in failed_user_agents:
            continue
        else:
            soup = try_agent(url, ua)
            if soup is not None:
                return soup, "requests"

    # Final fallback: Selenium
    print(f"[selenium fallback] {url}")
    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--log-level=3")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install(), log_path="/dev/null"), options=options)
        driver.get(url)
        time.sleep(3)
        html = driver.page_source
        driver.quit()
        total_bytes_downloaded += len(html.encode("utf-8"))
        total_requests_made += 1
        return BeautifulSoup(html, "html.parser"), "selenium"
    except Exception as e:
        print(f"[selenium error] {url} | {e}")
        return None, None


def try_agent(url, ua):
    global total_bytes_downloaded, total_requests_made
    headers = {"User-Agent": ua}
    try:
        time.sleep(random.uniform(2.0, 4.0))
        res = requests.get(url, headers=headers, timeout=5)
        total_requests_made += 1
        total_bytes_downloaded += len(res.content)
        if res.status_code == 200 and res.text.strip():
            log_user_agent(ua, success=True)
            return BeautifulSoup(res.text, "html.parser")
        else:
            log_user_agent(ua, success=False)
            return None
    except requests.exceptions.RequestException as e:
        log_user_agent(ua, success=False)
        return None
