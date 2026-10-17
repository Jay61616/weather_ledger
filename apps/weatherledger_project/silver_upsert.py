from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number

BRONZE = "local.weather_demo.bronze_events"
SILVER = "local.weather_demo.silver_observations"
spark = SparkSession.builder.appName("WeatherLedger-Silver").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

latest = Window.partitionBy("station_id", "observed_at").orderBy(
    col("revision").desc(), col("ingested_at").desc(), col("event_id").desc())
updates = (spark.table(BRONZE)
    .filter(col("temperature_c").between(-80, 60) &
            col("humidity_pct").between(0, 100) &
            (col("precipitation_mm") >= 0) & col("wind_speed_kmh").between(0, 400))
    .withColumn("rn", row_number().over(latest)).filter(col("rn") == 1).drop("rn"))
updates.createOrReplaceTempView("updates")

if not spark.catalog.tableExists(SILVER):
    updates.writeTo(SILVER).using("iceberg").create()
else:
    spark.sql(f"""
        MERGE INTO {SILVER} t USING updates s
        ON t.station_id = s.station_id AND t.observed_at = s.observed_at
        WHEN MATCHED AND (s.revision > t.revision OR
             (s.revision = t.revision AND s.ingested_at > t.ingested_at))
        THEN UPDATE SET t.event_id = s.event_id, t.ingested_at = s.ingested_at,
             t.temperature_c = s.temperature_c, t.humidity_pct = s.humidity_pct,
             t.precipitation_mm = s.precipitation_mm, t.wind_speed_kmh = s.wind_speed_kmh,
             t.revision = s.revision, t.source_batch_id = s.source_batch_id
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Silver current observations: {spark.table(SILVER).count():,}")
spark.stop()
