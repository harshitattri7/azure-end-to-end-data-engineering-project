# Databricks notebook source
# ── Cell 1: Load secrets from Key Vault ───────────────────────────────────────

SCOPE = "kv-ev-scope"

storage_account = dbutils.secrets.get(scope=SCOPE, key="adls-account-name")
sp_client_id = dbutils.secrets.get(scope=SCOPE, key="sp-client-id")
sp_client_secret = dbutils.secrets.get(scope=SCOPE, key="sp-client-secret")
sp_tenant_id = dbutils.secrets.get(scope=SCOPE, key="sp-tenant-id")

print(f"Storage account : {storage_account}")
print(f"SP client ID    : {sp_client_id[:8]}...[REDACTED]")
print(f"SP tenant ID    : {sp_tenant_id}")
print("All secrets loaded — OK")

# COMMAND ----------

# ── Cell 2: Set Spark OAuth config for this storage account ───────────────────

spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
    sp_client_id
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    sp_client_secret
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{sp_tenant_id}/oauth2/token"
)

print(f"Spark OAuth config set for: {storage_account}")
print("You can now read/write using abfss:// paths — no mount needed.")

# COMMAND ----------

# ── Cell 3: Path helper ───────────────────────────────────────────────────────

def abfss(container: str, path: str = "") -> str:
    """
    Returns a full abfss:// path.
    """
    base = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

print("Container paths:")

for container in ["bronze", "silver", "gold", "source"]:
    print(f"{container:<8} → {abfss(container)}")

# COMMAND ----------

# ── Cell 4: Verify read access to all 4 containers ───────────────────────────

print("=== Testing connection to all 4 containers ===\n")

all_ok = True

for container in ["bronze", "silver", "gold", "source"]:
    try:
        items = dbutils.fs.ls(abfss(container))
        print(f"{container:<8} OK — {len(items)} items")
    except Exception as e:
        print(f"{container:<8} ERROR — {e}")
        all_ok = False

print()

if all_ok:
    print("All 4 containers accessible — OAuth is working correctly.")
    print("You are ready to read and write data.")
else:
    print("One or more containers failed.")

# COMMAND ----------

# ── Cell 5: Test write access ─────────────────────────────

from datetime import datetime

test_file = abfss("bronze", f"healthcheck/test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")

dbutils.fs.put(test_file, "Storage connection successful!", overwrite=True)

print("Created:")
print(test_file)

print("\nListing folder:")
display(dbutils.fs.ls(abfss("bronze", "healthcheck")))