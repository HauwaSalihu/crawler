import os
import re
import requests
from requests.exceptions import RequestException
import time
import json
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from linkedin_fallback import search_linkedin_company
import urllib3

# 👇 Disable SSL warnings globally (since verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PHONE_REGEX = re.compile(
    r"(?:\+31\s?(?:\(0\))?\s?\d{1,2}[\s\-]?\d{6,7}|0\d{9})"
)

EXCLUDED_PATHS = [
    "blog", "news", "press", "article", "stories",
    "nieuws", "pers", "artikel", "verhalen"
]

EXCLUDED_DOMAINS = ["nltimes.nl"]
EXCLUDED_EXTENSIONS = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx")

# 🔁 Index tracker for sequential API/CSE rotation
_api_index = 0


def clean_dutch_phone(ph: str):
    cleaned = re.sub(r"[^\d+]", "", ph)
    if cleaned.startswith("+31") and 11 <= len(cleaned) <= 12:
        return cleaned
    if cleaned.startswith("0") and len(cleaned) == 10:
        return cleaned
    return None


def clean_company_name(title: str) -> str:
    if not title:
        return None
    parts = re.split(r"\s[-|–:]\s", title)
    return (parts[-1] if len(parts) > 1 else parts[0]).strip()


def safe_fetch(url, retries=3, delay=2):
    for attempt in range(retries):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36"
            }
            res = requests.get(url, headers=headers, timeout=10, verify=False)
            if res.status_code == 200:
                return res.text
            else:
                print(f"⚠️ {url} returned status {res.status_code}")
        except RequestException as e:
            print(f"⚠️ Error fetching {url}: {e} (attempt {attempt + 1}/{retries})")
            time.sleep(delay)
    print(f"⏭️ Skipping {url} after {retries} failed attempts")
    return None


def get_api_credentials(fallback=False):
    """
    Sequentially rotate through Google API keys and CSE IDs.
    If fallback=True, move to the next available key pair.
    """
    global _api_index
    api_keys = os.getenv("GOOGLE_API_KEYS", "").split(",")
    cse_ids = os.getenv("GOOGLE_CSE_IDS", "").split(",")

    api_keys = [k.strip() for k in api_keys if k.strip()]
    cse_ids = [c.strip() for c in cse_ids if c.strip()]

    if not api_keys or not cse_ids:
        raise ValueError("⚠️ Missing GOOGLE_API_KEYS or GOOGLE_CSE_IDS in .env")

    limit = min(len(api_keys), len(cse_ids))

    if fallback:
        _api_index = (_api_index + 1) % limit

    api_key = api_keys[_api_index % limit]
    cse_id = cse_ids[_api_index % limit]

    print(f"🔁 Using API key #{_api_index % limit + 1}: {api_key[:12]}... / CSE ID: {cse_id}")
    return api_key, cse_id


def crawl_companies(industry, region, staff_size=None, limit=50):
    global _api_index
    api_key, cse_id = get_api_credentials()

    variations = [
        "dakdekkers bedrijf Nederland",
        "dakbedekking aannemers Nederland",
        "dak installatie bedrijven Nederland",
        "dakdekkers Amsterdam",
        "dakdekkers Rotterdam",
        "dakdekkers Utrecht",
        "roofing companies Netherlands",
        "roofers Netherlands",
        "roofing contractors site:.nl",
        "dakdekkers bedrijf site:.nl",
    ]

    companies = []
    seen_urls = set()
    per_page = 10
    max_api_limit = 100

    for query in variations:
        for start in range(1, max_api_limit + 1, per_page):
            params = {
                "q": query,
                "cx": cse_id,
                "key": api_key,
                "num": per_page,
                "start": start,
                "gl": "nl",
                "hl": "nl"
            }
            try:
                res = requests.get("https://www.googleapis.com/customsearch/v1",
                                   params=params, timeout=10)
                data = res.json()

                # Handle API quota or error cases
                if res.status_code in [403, 429] or "error" in data:
                    print(f"⚠️ API key {_api_index + 1} failed — switching to next key...")
                    api_key, cse_id = get_api_credentials(fallback=True)
                    continue

            except RequestException as e:
                print(f"❌ Google CSE request failed for query '{query}': {e}")
                api_key, cse_id = get_api_credentials(fallback=True)
                continue

            if not data.get("items"):
                break

            for item in data.get("items", []):
                link = item.get("link", "")
                domain = urlparse(link).netloc.lower()
                path = urlparse(link).path.lower()

                if (
                    any(ext for ext in EXCLUDED_EXTENSIONS if link.endswith(ext))
                    or any(p in path for p in EXCLUDED_PATHS)
                    or any(domain.endswith(d) for d in EXCLUDED_DOMAINS)
                ):
                    continue

                if link and link not in seen_urls:
                    companies.append({
                        "url": link,
                        "title": clean_company_name(item.get("title")),
                        "snippet": item.get("snippet")
                    })
                    seen_urls.add(link)

            if len(companies) >= limit:
                return companies

        time.sleep(0.5)

    return companies


def enrich_company(url, region="Netherlands"):
    html = safe_fetch(url)
    if not html:
        return {
            "url": url,
            "company_name": None,
            "address": None,
            "emails": [],
            "phones": [],
            "linkedin_page": None,
            "staff_size": None,
            "raw": "no_html"
        }

    soup = BeautifulSoup(html, "html.parser")
    parsed = urlparse(url)
    path = parsed.path.lower().strip("/")
    if any(p in path for p in EXCLUDED_PATHS) or url.endswith(EXCLUDED_EXTENSIONS):
        return {
            "url": url,
            "company_name": parsed.netloc.split(".")[0].title(),
            "address": None,
            "emails": [],
            "phones": [],
            "linkedin_page": None,
            "staff_size": None,
            "raw": "Skipped publication/file page"
        }

    email_pattern = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.(?!png|jpg|jpeg|gif|bmp|svg|webp)[A-Za-z]{2,}"
    emails = list(set(re.findall(email_pattern, html)))[:5]

    raw_phones = PHONE_REGEX.findall(html)
    phones = list({p for p in (clean_dutch_phone(ph) for ph in raw_phones) if p})

    company_name, address = None, None
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") in ["LocalBusiness", "Organization"]:
                company_name = data.get("name", company_name)
                addr = data.get("address")
                if isinstance(addr, dict):
                    address = ", ".join(filter(None, [
                        addr.get("streetAddress"),
                        addr.get("postalCode"),
                        addr.get("addressLocality"),
                        addr.get("addressCountry")
                    ]))
        except Exception:
            continue

    if not company_name:
        if soup.title:
            company_name = clean_company_name(soup.title.string.strip())
        elif soup.h1:
            company_name = clean_company_name(soup.h1.get_text().strip())

    try:
        linkedin_page, staff_size = search_linkedin_company(company_name or url, region=region)
    except Exception:
        linkedin_page, staff_size = None, None

    return {
        "url": url,
        "company_name": company_name,
        "address": address,
        "emails": emails,
        "phones": phones,
        "linkedin_page": linkedin_page,
        "staff_size": staff_size,
        "raw": f"Extracted {len(emails)} emails & {len(phones)} phones"
    }