# Databricks notebook source
import requests

SCOPE = "kv-ev-scope"

api_base_url = dbutils.secrets.get(scope=SCOPE, key="voltgrid-api-base-url")
username     = dbutils.secrets.get(scope=SCOPE, key="voltgrid-username")
password     = dbutils.secrets.get(scope=SCOPE, key="voltgrid-password")

print(f"API base URL : {api_base_url}")
print(f"Username     : {username}")
print(f"Password     : [REDACTED]")
print("Credentials loaded from Key Vault — OK")

# COMMAND ----------

resp = requests.post(
    f"{api_base_url}/api/auth/login/",
    json={"username": username, "password": password},
    timeout=10,
)
resp.raise_for_status()
token = resp.json()["token"]

print(f"Login response status : {resp.status_code}")
print(f"Token acquired        : {token[:8]}...[REDACTED]")
print("API login — OK")

API_TOKEN   = token
API_HEADERS = {"Authorization": f"Token {API_TOKEN}"}

# COMMAND ----------

r = requests.get(
    f"{api_base_url}/api/db/payments/?page=1&page_size=5",
    headers=API_HEADERS,
    timeout=10,
)
r.raise_for_status()
data = r.json()

pg = data.get("pagination", {})
print(f"Total records        : {pg.get('total', 'N/A'):,}")
print(f"Total pages          : {pg.get('total_pages', 'N/A'):,}")
print(f"Page size            : {pg.get('page_size', 'N/A')}")
print(f"Records in this page : {len(data.get('data', []))}")

print(f"\nSample record:")
records = data.get("data", [])
if records:
    for k, v in records[0].items():
        print(f"  {k:<25} : {v}")

print("\nPayments API call — OK")

# COMMAND ----------

ENDPOINTS = [
    "payments", "sessions", "customers", "fleet", "chargers",
    "vehicles", "stations", "complaints", "maintenance-events",
    "energy-prices", "tariffs", "charge-cards", "employees",
    "partners", "cities", "states", "weather", "pipeline-audit"
]

print(f"{'Endpoint':<25} {'Status':>8} {'Total Rows':>12} {'Total Pages':>13}")
print("-" * 65)

endpoint_errors = []
for ep in ENDPOINTS:
    try:
        r = requests.get(
            f"{api_base_url}/api/db/{ep}/?page=1&page_size=1",
            headers=API_HEADERS,
            timeout=10,
        )
        if r.status_code == 200:
            pg    = r.json().get("pagination", {})
            total = pg.get("total", 0)
            pages = pg.get("total_pages", 0)
            print(f"{ep:<25} {'200 OK':>8} {total:>12,} {pages:>13,}")
        else:
            print(f"{ep:<25} {r.status_code:>8} {'ERROR':>12}")
            endpoint_errors.append(ep)
    except Exception as e:
        print(f"{ep:<25} {'FAIL':>8} {str(e)[:30]:>12}")
        endpoint_errors.append(ep)

print("-" * 65)
if endpoint_errors:
    print(f"\nEndpoints with errors: {endpoint_errors}")
    print("Re-run Cell 2 to refresh the token if you see 401 errors.")
else:
    print(f"\nAll {len(ENDPOINTS)} endpoints reachable — API auth verified.")

# COMMAND ----------

r = requests.get(
    f"{api_base_url}/api/db/payments/?page=1&page_size=500",
    headers=API_HEADERS,
    timeout=30,
)
r.raise_for_status()
recs = r.json().get("data", [])

if not recs:
    print("ERROR: No records returned from payments endpoint.")
    print("  → Check the raw response keys: print(r.json().keys())")
    print("  → Re-run Cell 2 to get a fresh token and try again.")
else:
    VALID_STATUS = {"Success", "Failed", "Pending", "Retry", "Refunded", "Disputed"}

    neg_amount  = [x for x in recs if float(x.get("amount_aud", 0) or 0) < 0]
    zero_amount = [x for x in recs if float(x.get("amount_aud", 0) or 0) == 0]
    bad_status  = [x for x in recs if x.get("status", "") not in VALID_STATUS]

    total = len(recs)
    print(f"\nNoise check on {total} payment records:")
    print(f"  Negative amounts  : {len(neg_amount):>5} ({len(neg_amount)/total*100:.1f}%) — expected ~5%")
    print(f"  Zero amounts      : {len(zero_amount):>5} ({len(zero_amount)/total*100:.1f}%) — expected ~5%")
    print(f"  Invalid status    : {len(bad_status):>5} ({len(bad_status)/total*100:.1f}%) — expected ~5%")

    if neg_amount:
        s = neg_amount[0]
        print(f"\n  Sample negative: payment_id={s.get('payment_id')}, amount={s.get('amount_aud')}")
    if bad_status:
        s = bad_status[0]
        print(f"  Sample bad status: payment_id={s.get('payment_id')}, status='{s.get('status')}'")

    print("\nNoise check complete — Silver layer will clean these in Day 7.")