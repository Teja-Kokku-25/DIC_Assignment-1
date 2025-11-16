# Silver Layer - Fixed (no distinct on map columns)
# Workspace: /Workspace/Users/yuvamani@buffalo.edu/Assignment-1/

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("CVE_Silver").getOrCreate()

print("=" * 80)
print("SILVER LAYER")
print("=" * 80)

# Read Bronze
print("\n[1] Reading Bronze table...")
df_bronze = spark.sql("SELECT * FROM main.cve_bronze.records")
bronze_count = df_bronze.count()
print(f"✓ {bronze_count:,} records")

# Core CVE table
print("\n[2] Creating core CVE table...")
df_cve_core = df_bronze.select(
    get_json_object(col("data")["cveMetadata"], "$.ID").alias("cve_id"),
    get_json_object(col("data")["cveMetadata"], "$.datePublished").alias("date_published"),
    get_json_object(col("data")["cveMetadata"], "$.state").alias("state"),
    get_json_object(col("data")["containers.cna"], "$.title").alias("title"),
    lit("cveproject/cvelistv5").alias("source"),
    current_timestamp().alias("ingestion_timestamp")
).distinct()

core_count = df_cve_core.count()
print(f"✓ {core_count:,} core records")

# Write core table
print("\n[3] Saving core CVE table...")
df_cve_core.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("main.cve_silver.cve_core")
print("✓ Saved")

# Affected products - NO distinct() on map columns
print("\n[4] Creating affected products table...")
df_affected = df_bronze.select(
    get_json_object(col("data")["cveMetadata"], "$.ID").alias("cve_id"),
    col("data").alias("raw_data")
)

affected_count = df_affected.count()
print(f"✓ {affected_count:,} records")

# Write affected table - NO distinct
print("\n[5] Saving affected products table...")
df_affected.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("main.cve_silver.affected_products")
print("✓ Saved")

# Verify
print("\n[6] Verification...")
core_final = spark.sql("SELECT COUNT(*) as cnt FROM main.cve_silver.cve_core").collect()[0][0]
affected_final = spark.sql("SELECT COUNT(*) as cnt FROM main.cve_silver.affected_products").collect()[0][0]

print(f"✓ Core CVE records: {core_final:,}")
print(f"✓ Affected products: {affected_final:,}")

print("\n" + "=" * 80)
print("✓ SILVER LAYER COMPLETE")
print("=" * 80)
