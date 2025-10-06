# crawler/linkedin_fallback.py
import os
import re
import requests
import time
from urllib.parse import unquote

API_URL = "https://www.googleapis.com/customsearch/v1"


def _query_google_cse(q, cx, key, num=3):
    """
    Query Google Custom Search Engine (CSE).
    Always returns a dict with {items: [...], error: str|None}.
    """
    try:
        r = requests.get(API_URL, params={"q": q, "cx": cx, "key": key, "num": num}, timeout=10)
        if r.status_code != 200:
            return {"items": [], "error": r.text}
        data = r.json()
        return {"items": data.get("items", []), "error": data.get("error")}
    except Exception as e:
        return {"items": [], "error": str(e)}


def _normalize_linkedin_url(url):
    """
    Basic cleanup of LinkedIn URLs returned by Google.
    Removes Google tracking and URL-encodings.
    """
    if not url:
        return url
    u = url.split("&")[0]  # remove Google tracking params
    return unquote(u)


def _extract_name_from_title(title):
    """
    Try to extract a person's name from a Google result title.
    E.g. "Jane Doe - CEO at RoofCo | LinkedIn"
    """
    if not title:
        return None
    candidate = title.strip()

    if " - " in candidate:
        parts = candidate.split(" - ")
        if parts:
            return parts[0].strip()

    if "|" in candidate:
        parts = candidate.split("|")
        if parts:
            return parts[0].strip()

    return None


def search_linkedin_management(company_name, region="Netherlands", roles=None, attempts=1, sleep_between=0.5):
    """
    Query Google CSE for LinkedIn profiles/pages related to company_name + roles.
    Returns:
      {
        "company": { "query": ..., "region": ... },
        "management_team": [
          { name, roles_found, linkedin_url, snippet, source_queries }
        ]
      }
    """
    api_key = os.getenv("GOOGLE_API_KEY")
    cse_id = os.getenv("GOOGLE_CSE_ID")
    if not api_key or not cse_id:
        raise ValueError("Missing required env vars: GOOGLE_API_KEY and/or GOOGLE_CSE_ID")

    if roles is None:
        roles = [
            "CEO", "Chief Executive Officer", "Founder", "Managing Director",
            "COO", "CFO", "CTO", "Director", "Head of", "Chair", "President"
        ]

    results = []
    seen_urls = set()
    company_record = {"query": company_name, "region": region}

    for role in roles:
        q = f'site:linkedin.com "{company_name}" "{role}" {region}'

        data = None
        for attempt in range(attempts):
            data = _query_google_cse(q, cse_id, api_key, num=3)
            if data["items"] or attempt >= attempts - 1:
                break
            time.sleep(1)  # retry backoff

        for it in data.get("items", []):
            link = _normalize_linkedin_url(it.get("link") or it.get("formattedUrl") or "")
            if "linkedin.com" not in link:
                continue

            key = link.split("?")[0]
            if key in seen_urls:
                continue
            seen_urls.add(key)

            title_text = it.get("title", "")
            snippet = it.get("snippet", "")

            name_guess = _extract_name_from_title(title_text)
            results.append({
                "name": name_guess,
                "title": role,
                "linkedin_url": link,
                "snippet": snippet,
                "source_query": q
            })

        time.sleep(sleep_between)

    # Consolidate by linkedin_url
    consolidated = {}
    for r in results:
        key = r["linkedin_url"].split("?")[0]
        if key not in consolidated:
            consolidated[key] = {
                "name": r.get("name"),
                "linkedin_url": r["linkedin_url"],
                "snippet": r.get("snippet"),
                "roles_found": [r["title"]],
                "source_queries": [r["source_query"]],
            }
        else:
            consolidated[key]["roles_found"].append(r["title"])
            consolidated[key]["source_queries"].append(r["source_query"])
            if not consolidated[key].get("name") and r.get("name"):
                consolidated[key]["name"] = r["name"]

    return {
        "company": company_record,
        "management_team": list(consolidated.values())
    }


def search_linkedin_company(company_name, region="Netherlands"):
    """
    Query LinkedIn company pages via Google CSE.
    Returns (first_match_url, staff_size) or (None, None).
    """
    q = f'site:linkedin.com/company "{company_name}" {region}'
    data = _query_google_cse(q, os.getenv("GOOGLE_CSE_ID"), os.getenv("GOOGLE_API_KEY"))

    for item in data.get("items", []):
        link = _normalize_linkedin_url(item.get("link"))
        snippet = item.get("snippet", "")

        staff_size = None
        match = re.search(r"(\d{1,4})\+?\s*employees", snippet, re.I)
        if match:
            staff_size = int(match.group(1))

        if "linkedin.com/company" in link:
            return link, staff_size

    return None, None
