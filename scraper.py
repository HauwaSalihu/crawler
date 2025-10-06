import os
import re
import requests
from requests.exceptions import RequestException
import time
import json
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from linkedin_fallback import search_linkedin_company

PHONE_REGEX = re.compile(
    r"(?:\+31\s?(?:\(0\))?\s?\d{1,2}[\s\-]?\d{6,7}|0\d{9})"
)

EXCLUDED_PATHS = [
    "blog", "news", "press", "article", "stories",
    "nieuws", "pers", "artikel", "verhalen"
]

EXCLUDED_DOMAINS = ["nltimes.nl"]
EXCLUDED_EXTENSIONS = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx")


def clean_dutch_phone(ph: str):
    cleaned = re.sub(r"[^\d+]", "", ph)  # keep only digits and +
    if cleaned.startswith("+31") and 11 <= len(cleaned) <= 12:
        return cleaned
    if cleaned.startswith("0") and len(cleaned) == 10:
        return cleaned
    return None


def clean_company_name(title: str) -> str:
    """
    Clean company names from page titles by removing marketing suffixes/prefixes.
    """
    if not title:
        return None

    # Remove common separators and trailing/leading marketing text
    parts = re.split(r"\s[-|–:]\s", title)
    if len(parts) > 1:
        # Heuristic: last part is most likely the real name
        candidate = parts[-1]
    else:
        candidate = parts[0]

    return candidate.strip()


def safe_fetch(url, retries=3, delay=2):
    for attempt in range(retries):
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                                     "Chrome/120.0.0.0 Safari/537.36"}
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code == 200:
                return res.text
            else:
                print(f"⚠️ {url} returned status {res.status_code}")
        except RequestException as e:
            print(f"⚠️ Error fetching {url}: {e} (attempt {attempt+1}/{retries})")
            time.sleep(delay)
    print(f"⏭️ Skipping {url} after {retries} failed attempts")
    return None


def crawl_companies(industry, region, staff_size=None, limit=180):
    api_key = os.getenv("GOOGLE_API_KEY")
    cse_id = os.getenv("GOOGLE_CSE_ID")

    if not api_key or not cse_id:
        raise ValueError("⚠️ Missing GOOGLE_API_KEY or GOOGLE_CSE_ID in .env")

    variations = [
        # 🇳🇱 Dutch (primary)
        "dakdekkers bedrijf Nederland",
        "dakbedekking aannemers Nederland",
        "dak installatie bedrijven Nederland",
        "dakdekkers Amsterdam",
        "dakdekkers Rotterdam",
        "dakdekkers Utrecht",
        "dakdekkers Groningen",
        "dakdekkers Eindhoven",

        # 🇬🇧 English (targeting Dutch companies)
        "roofing contractors Netherlands",
        "roofing companies Netherlands",
        "roof installation companies Netherlands",
        "roof repair services Netherlands",
        "roofers Amsterdam Netherlands",
        "roofers Rotterdam Netherlands",
        "roofers Utrecht Netherlands",
        "roofers Groningen Netherlands",
        "roofers Eindhoven Netherlands",

        # 🌍 Domain-focused searches (.nl bias)
        "roofing contractors site:.nl",
        "dakdekkers bedrijf site:.nl",
        "dakbedekking aannemers site:.nl",
        "dak installatie bedrijven site:.nl",
        "roof installation companies site:.nl",

        # 🧠 Broader commercial/industry terms
        "dak onderhoud bedrijven Nederland",
        "dakisolatie bedrijven Nederland",
        "dakreparatie bedrijven Nederland",
        "roof maintenance companies Netherlands",
        "roof insulation contractors Netherlands",

        # 💼 Directories & listings
        "roofing company directory Netherlands",
        "dakdekkers gids Nederland",
        "bouwbedrijven Nederland dakbedekking",
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
                "start": start
            }
            try:
                res = requests.get("https://www.googleapis.com/customsearch/v1",
                                   params=params, timeout=10)
                data = res.json()
            except RequestException as e:
                print(f"❌ Google CSE request failed for query '{query}': {e}")
                continue

            if not data.get("items"):
                break

            for item in data.get("items", []):
                link = item.get("link", "")
                domain = urlparse(link).netloc.lower()
                path = urlparse(link).path.lower()

                # Skip excluded domains, extensions, and paths
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

    # ---------- Extract emails ----------
    email_pattern = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.(?!png|jpg|jpeg|gif|bmp|svg|webp)[A-Za-z]{2,}"
    emails = list(set(re.findall(email_pattern, html)))[:5]

    # ---------- Extract phones ----------
    raw_phones = PHONE_REGEX.findall(html)
    phones = list({p for p in (clean_dutch_phone(ph) for ph in raw_phones) if p})

    # ---------- Extract company name + address ----------
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
