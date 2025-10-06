import os, redis, json, time
from scraper import crawl_companies, enrich_company
from linkedin_fallback import search_linkedin_management as search_linkedin_ceo
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import ObjectId

# Load env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

# Setup connections
try:
    mongo_uri = os.getenv("MONGO_URI")
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        raise SystemExit("❌ REDIS_URL not set in environment!")

    r = redis.from_url(redis_url, decode_responses=True, ssl=redis_url.startswith("rediss://"))
    mongo = MongoClient(mongo_uri)
    db = mongo["crawler"]
    searches = db["searches"]
except Exception as e:
    print(f"❌ Failed to connect to Redis/Mongo: {e}")
    raise SystemExit(1)

print("📡 Crawler worker started...")

while True:
    try:
        job = r.brpop("jobs", timeout=5)  # ⏳ wait up to 5s
    except Exception as e:
        print(f"⚠️ Redis error: {e}")
        time.sleep(2)
        continue

    if not job:
        # no job in timeout window → loop again
        continue

    _, payload = job
    data = json.loads(payload)
    print(f"⚡ Processing job: {data}")

    job_id = data.get("jobId")
    try:
        job_id = ObjectId(job_id)
    except Exception:
        pass  # keep as string if not ObjectId

    # 1️⃣ Crawl raw companies
    companies = crawl_companies(data["industry"], data["region"], limit=480)
    print(f"🔎 Found {len(companies)} raw companies")

    searches.update_one(
        {"_id": job_id},
        {"$set": {"status": "crawled", "raw": companies}},
        upsert=True,
    )

    # 2️⃣ Enrichment loop
    enriched = []
    for c in companies:
        site_info, linkedin_info = {}, {}
        try:
            site_info = enrich_company(c["url"])
        except Exception as e:
            print(f"⚠️ enrich_company failed for {c['url']}: {e}")

        try:
            linkedin_info = search_linkedin_ceo(c["title"], data["region"])
        except Exception as e:
            print(f"⚠️ LinkedIn CEO lookup failed for {c['title']}: {e}")

        enriched.append({**c, "site": site_info, "linkedin": linkedin_info})

    # 3️⃣ Save results
    searches.update_one(
        {"_id": job_id},
        {"$set": {"status": "done", "results": enriched if enriched else companies}},
        upsert=True,
    )

    # 4️⃣ Also save to file
    output_path = os.path.join(
        os.path.dirname(__file__), f"results_{data['jobId']}.json"
    )
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            {"raw": companies, "results": enriched if enriched else companies},
            f,
            indent=2,
            ensure_ascii=False,
        )

    print(f"📝 Results saved to {output_path} ({len(companies)} raw, {len(enriched)} enriched)")
