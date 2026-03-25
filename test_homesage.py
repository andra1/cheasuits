"""
Quick test of Homesage.ai API against properties in the database.
Run: python3 test_homesage.py
"""
import os
import json
import sqlite3
import urllib.request
import urllib.parse
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("homesage_api_key")
BASE_URL = "https://developers.homesage.ai/api/properties"
DB_PATH = "data/cheasuits.db"


def api_get(endpoint: str, params: dict) -> dict:
    """Make authenticated GET request to Homesage API."""
    qs = urllib.parse.urlencode(params)
    url = f"{BASE_URL}/{endpoint}/?{qs}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {API_KEY}",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def get_test_properties(limit=3):
    """Pull a few properties with addresses from the DB."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, property_address, assessed_value, owner_name "
        "FROM properties WHERE property_address IS NOT NULL LIMIT ?",
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def clean_address(raw: str) -> str:
    """Convert multi-line DB address to single-line format."""
    return raw.replace("\n", ", ").strip()


def main():
    print(f"API Key loaded: {'yes' if API_KEY else 'NO — check .env'}\n")

    properties = get_test_properties(3)

    for pid, addr_raw, assessed, owner in properties:
        addr = clean_address(addr_raw)
        print(f"{'='*60}")
        print(f"Property #{pid}: {addr}")
        print(f"Owner: {owner} | County Assessed: ${assessed:,.0f}")
        print(f"{'='*60}")

        # 1) Basic Info (free tier)
        try:
            basic = api_get("basic-info", {"property_address": addr})
            print("\n--- Basic Info ---")
            print(json.dumps(basic, indent=2)[:2000])
        except Exception as e:
            print(f"\nBasic Info ERROR: {e}")

        # 2) Full Property Info (paid)
        try:
            info = api_get("info", {"property_address": addr})
            print("\n--- Property Info ---")
            print(json.dumps(info, indent=2)[:2000])
        except Exception as e:
            print(f"\nProperty Info ERROR: {e}")

        # 3) Comps (paid)
        try:
            comps = api_get("comps", {"property_address": addr})
            print("\n--- Comps ---")
            print(json.dumps(comps, indent=2)[:2000])
        except Exception as e:
            print(f"\nComps ERROR: {e}")

        # 4) AVM / Valuation (try common endpoint names)
        for endpoint in ["avm", "valuation", "estimate"]:
            try:
                val = api_get(endpoint, {"property_address": addr})
                print(f"\n--- {endpoint.upper()} ---")
                print(json.dumps(val, indent=2)[:1500])
                break
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    continue  # endpoint doesn't exist, try next
                print(f"\n{endpoint.upper()} ERROR: {e}")
                break
            except Exception as e:
                print(f"\n{endpoint.upper()} ERROR: {e}")
                break

        print("\n")


if __name__ == "__main__":
    main()
