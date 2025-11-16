# BRONZE LAYER - Let Databricks manage location
# Workspace: /Workspace/Users/yuvamani@buffalo.edu/Assignment-1/

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
import subprocess
import json
import glob

spark = SparkSession.builder.appName("CVE_Bronze").getOrCreate()

# Verify catalog
try:
    spark.sql("SHOW CATALOGS").collect()
except:
    raise Exception("Run 00_setup_create_catalog.sql first!")

# Drop old table
spark.sql("DROP TABLE IF EXISTS main.cve_bronze.records")
print("Dropped old table")

# Clone
repo = "/tmp/cvelistV5"
subprocess.run(f"rm -rf {repo}", shell=True, capture_output=True)
subprocess.run(
    f"git clone --depth 1 https://github.com/CVEProject/cvelistV5.git {repo}",
    shell=True, capture_output=True, text=True, timeout=300
)

# Load
files = sorted(glob.glob(f"{repo}/cves/2024/**/CVE-2024-*.json", recursive=True))
print(f"Found {len(files):,} files")

json_strings = []
for f in files:
    try:
        with open(f) as fp:
            json_strings.append(json.dumps(json.load(fp)))
    except:
        pass

print(f"Loaded {len(json_strings):,} records")

# Create DataFrame
df = spark.createDataFrame([(s,) for s in json_strings], ["json_str"])
df = df.select(from_json("json_str", "map<string,string>").alias("data"))

print(f"DataFrame: {df.count():,} rows")

# Write directly to table in catalog - NO custom LOCATION
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("main.cve_bronze.records")

print("Saved as table")

# Verify
final = spark.sql("SELECT COUNT(*) as cnt FROM main.cve_bronze.records").collect()[0][0]
print(f"✓ DONE: {final:,} records")

# Cleanup
subprocess.run(f"rm -rf {repo}", shell=True, capture_output=True)
