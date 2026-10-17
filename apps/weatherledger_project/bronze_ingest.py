import csv
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

NAMESPACE = "local.weather_demo"
TABLE = f"{NAMESPACE}.bronze_events"

DATA_DIR = "/opt/spark-data/weatherledger_project/landing"
BASELINE = os.path.join(DATA_DIR, "weather_baseline.csv")
CHANGES = os.path.join(DATA_DIR, "weather_changes.csv")


def get_source_batch_id(path):

    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Input file not found: {path}. Run generate_demo_data.py first."
        )

    with open(path, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        first_row = next(reader, None)

        if first_row is None:
            raise ValueError(f"Input file has no data rows: {path}")

        batch_id = (first_row.get("source_batch_id") or "").strip()

        if not batch_id:
            raise ValueError(
                f"Missing source_batch_id in input file: {path}"
            )

    return batch_id

# Discover batch IDs from the actual input files.

baseline_batch_id = get_source_batch_id(BASELINE)
changes_batch_id = get_source_batch_id(CHANGES)

spark = (
    SparkSession.builder
    .appName("WeatherLedger-Bronze")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")

# Find the source batches already represented in Bronze.
loaded = set()

if spark.catalog.tableExists(TABLE):
    loaded = {
        row.source_batch_id
        for row in (
            spark.table(TABLE)
            .select("source_batch_id")
            .distinct()
            .collect()
        )
    }

# Load the baseline first, then the changes batch.
if baseline_batch_id not in loaded:
    path = BASELINE
elif changes_batch_id not in loaded:
    path = CHANGES
else:
    print(
        "Both input batches are already in Bronze; no changes made."
    )
    spark.stop()
    raise SystemExit(0)

# Read the selected CSV into a Spark DataFrame.
events = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(path)
    .withColumn("observed_at", to_timestamp("observed_at"))
    .withColumn("ingested_at", to_timestamp("ingested_at"))
)

batch_id = events.select("source_batch_id").first()[0]

# Create the table on the first load; append subsequent batches.
if not spark.catalog.tableExists(TABLE):
    events.writeTo(TABLE).using("iceberg").create()
else:
    events.writeTo(TABLE).append()

print(f"Loaded {batch_id}: {events.count():,} events")
print(f"Bronze total: {spark.table(TABLE).count():,}")

spark.stop()
