# Databricks notebook source
SCOPE = "kv-ev-scope"

STORAGE_ACCOUNT = dbutils.secrets.get(scope=SCOPE, key="source-storage-account")
CONTAINER       = dbutils.secrets.get(scope=SCOPE, key="source-container")
SAS_TOKEN       = dbutils.secrets.get(scope=SCOPE, key="source-sas-token")

print(f"Storage account : {STORAGE_ACCOUNT}")
print(f"Container       : {CONTAINER}")
print(f"SAS token       : [REDACTED]")
print("Credentials loaded from Key Vault — OK")

spark.conf.set(
    f"fs.azure.sas.{CONTAINER}.{STORAGE_ACCOUNT}.blob.core.windows.net",
    SAS_TOKEN
)

print("Spark SAS config set — OK")

# COMMAND ----------

base_path = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/"

items = dbutils.fs.ls(base_path)

for item in items:
    item_type = "DIR " if item.size == 0 else "FILE"
    print(f"  [{item_type}] {item.name:<50} {item.size:>10} bytes")

print(f"\n  Total: {len(items)} items found at root level")

# COMMAND ----------

sessions_root = (
    f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"
    f"/realtime/charging_sessions/"
)

items = dbutils.fs.ls(sessions_root)
print("Level 1 — contents of charging_sessions/:")
for item in items:
    print(f"  {item.name}")

print("\nLevel 2 — drilling one folder deeper:")
for item in items:
    try:
        sub_items = dbutils.fs.ls(item.path)
        for s in sub_items:
            print(f"  {item.name}{s.name}")
    except:
        pass

# COMMAND ----------

csv_path = (
    f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"
    f"/realtime/charging_sessions/2026/06/01/06/sessions_20260601_0600.csv"
)

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(csv_path)
)

print(f"Row count    : {df.count():,}")
print(f"Column count : {len(df.columns)}")
print(f"Columns      : {df.columns}\n")
df.printSchema()
display(df.limit(10))

# COMMAND ----------

base      = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"
glob_path = f"{base}/realtime/charging_sessions/*/*/*/*/*.csv"

df_all = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(glob_path)
)

total_rows = df_all.count()
print(f"Total rows across all files : {total_rows:,}")
print(f"Column count                : {len(df_all.columns)}")
display(df_all.limit(10))

# COMMAND ----------

import pyspark.sql.functions as F

total = df_all.count()
print(f"Total rows : {total:,}\n")

for col_name in df_all.columns:
    null_count = df_all.filter(
        F.col(col_name).isNull() | (F.col(col_name).cast("string") == "")
    ).count()

    pct  = (null_count / total * 100) if total > 0 else 0
    flag = "<-- check this" if pct > 10 else ""
    print(f"  {col_name:<35} {null_count:>8,}  {pct:>5.1f}%  {flag}")