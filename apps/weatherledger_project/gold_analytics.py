import csv
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, max, round, sum, to_date

SILVER = "local.weather_demo.silver_observations"
GOLD = "local.weather_demo.gold_daily_summary"
OUTPUT = "/opt/spark-data/weatherledger_project/powerbi/daily_weather_summary.csv"
spark = SparkSession.builder.appName("WeatherLedger-Gold").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

daily = (spark.table(SILVER).withColumn("observation_date", to_date("observed_at"))
    .groupBy("station_id", "observation_date")
    .agg(count("event_id").alias("observation_count"),
         round(avg("temperature_c"), 2).alias("avg_temperature_c"),
         round(avg("humidity_pct"), 2).alias("avg_humidity_pct"),
         round(sum("precipitation_mm"), 2).alias("total_precipitation_mm"),
         round(max("wind_speed_kmh"), 2).alias("max_wind_speed_kmh")))
daily.createOrReplaceTempView("daily_updates")

if not spark.catalog.tableExists(GOLD):
    daily.writeTo(GOLD).using("iceberg").create()
else:
    spark.sql(f"""
        MERGE INTO {GOLD} t USING daily_updates s
        ON t.station_id = s.station_id AND t.observation_date = s.observation_date
        WHEN MATCHED AND (t.observation_count <> s.observation_count OR
             t.avg_temperature_c <> s.avg_temperature_c OR
             t.avg_humidity_pct <> s.avg_humidity_pct OR
             t.total_precipitation_mm <> s.total_precipitation_mm OR
             t.max_wind_speed_kmh <> s.max_wind_speed_kmh)
        THEN UPDATE SET t.observation_count = s.observation_count,
             t.avg_temperature_c = s.avg_temperature_c,
             t.avg_humidity_pct = s.avg_humidity_pct,
             t.total_precipitation_mm = s.total_precipitation_mm,
             t.max_wind_speed_kmh = s.max_wind_speed_kmh
        WHEN NOT MATCHED THEN INSERT *
    """)

os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
rows = spark.table(GOLD).orderBy("observation_date", "station_id").collect()
with open(OUTPUT, "w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=list(rows[0].asDict().keys()))
    writer.writeheader()
    writer.writerows(row.asDict() for row in rows)

print(f"Gold rows: {len(rows):,}; Power BI file: {OUTPUT}")
spark.stop()
