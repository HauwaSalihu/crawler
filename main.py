import os, redis, json, time, threading
from scraper import crawl_companies, enrich_company
from linkedin_fallback import search_linkedin_management as search_linkedin_ceo
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import ObjectId
from http.server import HTTPServer, BaseHTTPRequestHandler

# Load env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

# Setup connections
try:
    mongo_uri = os.getenv("MONGO_URI")
    redis_url = os.getenv("REDIS_URL")

    r = redis.from_url(redis_url, decode_responses=True)
    mongo = MongoClient(mongo_uri)
    db = mongo["crawler"]
    searches = db["searches"]
except Exception as e:
    print(f"❌ Failed to connect to Redis/Mongo: {e}")
    raise SystemExit(1)

print("📡 Crawler worker starting...")

# ----------------- Health Server -----------------
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write("Worker running".encode("utf-8"))

def run_health_server():
    port = int(os.environ.get("PORT", 8000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"🌍 Health server running on port {port}")
    server.serve_forever()

# ----------------- Worker Loop -----------------
def start_crawler():
    while True:
        try:
            job = r.brpop("jobs", timeout=5)  # ⏳ wait up to 5s
        except Exception as e:
            print(f"⚠️ Redis error: {e}")
            time.sleep(2)
            continue

        if not job:
            continue  # no job in timeout window → loop again

        _, payload = job
        data = json.loads(payload)
        print(f"⚡ Processing job: {data}")

        job_id = data.get("jobId")
        try:
            job_id = ObjectId(job_id)
        except Exception:
            pass  # keep as string if not ObjectId

        # 1️⃣ Crawl raw companies
        companies = crawl_companies(data["industry"], data["region"], limit=80)
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

        print(f"📝 Job {job_id} done — {len(companies)} raw, {len(enriched)} enriched")

# ----------------- Entry -----------------
if __name__ == "__main__":
    # Start health server in background
    threading.Thread(target=run_health_server, daemon=True).start()

    # Start crawler worker (blocking loop)
    start_crawler()