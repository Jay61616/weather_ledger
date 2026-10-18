# ============================================================
# WeatherLedger - Complete SQL Verification Queries
# ============================================================

# Replace this with the Silver snapshot ID saved BEFORE
# ingesting weather_changes.csv.
silver_baseline_snapshot_id = 1234567890123456789


# ============================================================
# 1. AFTER THE BASELINE BRONZE LOAD
# ============================================================

# Query 1: Count events by source batch
spark.sql("""
    SELECT
        source_batch_id,
        COUNT(*) AS received_events
    FROM local.weather_demo.bronze_events
    GROUP BY source_batch_id
""").show(truncate=False)


# Query 2: Inspect the original ST-001 observation in Bronze
spark.sql("""
    SELECT
        station_id,
        observed_at,
        temperature_c,
        revision,
        event_id
    FROM local.weather_demo.bronze_events
    WHERE station_id = 'ST-001'
      AND observed_at = TIMESTAMP '2024-06-15 12:00:00'
""").show(truncate=False)


# ============================================================
# 2. AFTER THE FIRST SILVER BUILD
# ============================================================

# Query 3: Inspect Silver snapshot history
# Save the baseline snapshot ID before ingesting changes.
spark.sql("""
    SELECT
        committed_at,
        snapshot_id,
        operation
    FROM local.weather_demo.silver_observations.snapshots
    ORDER BY committed_at DESC
""").show(truncate=False)


# Query 4: Verify that Silver contains no duplicate current keys
spark.sql("""
    SELECT
        COUNT(*) AS duplicate_current_keys
    FROM (
        SELECT
            station_id,
            observed_at
        FROM local.weather_demo.silver_observations
        GROUP BY station_id, observed_at
        HAVING COUNT(*) > 1
    ) duplicates
""").show(truncate=False)


# ============================================================
# 3. AFTER RUNNING bronze_ingest.py A SECOND TIME
# ============================================================

# Query 5: Inspect records from the changes batch in Bronze
spark.sql("""
    SELECT
        event_id,
        station_id,
        observed_at,
        temperature_c,
        revision,
        ingested_at
    FROM local.weather_demo.bronze_events
    WHERE source_batch_id = 'weather-changes-2024-10-01'
    ORDER BY station_id, observed_at, ingested_at
""").show(truncate=False)


# Query 6: Count delivered copies of the ST-002 observation
spark.sql("""
    SELECT
        station_id,
        observed_at,
        COUNT(*) AS delivered_copies
    FROM local.weather_demo.bronze_events
    WHERE station_id = 'ST-002'
      AND observed_at = TIMESTAMP '2024-06-20 08:00:00'
    GROUP BY station_id, observed_at
""").show(truncate=False)


# ============================================================
# 4. AFTER RERUNNING silver_upsert.py
# ============================================================

# Query 7: Inspect all three observations in Silver
spark.sql("""
    SELECT
        station_id,
        observed_at,
        temperature_c,
        revision,
        event_id,
        ingested_at
    FROM local.weather_demo.silver_observations
    WHERE (
        station_id = 'ST-001'
        AND observed_at = TIMESTAMP '2024-06-15 12:00:00'
    )
    OR (
        station_id = 'ST-003'
        AND observed_at = TIMESTAMP '2024-06-15 12:30:00'
    )
    OR (
        station_id = 'ST-002'
        AND observed_at = TIMESTAMP '2024-06-20 08:00:00'
    )
    ORDER BY station_id, observed_at
""").show(truncate=False)


# ============================================================
# 5. APACHE ICEBERG TIME TRAVEL
# ============================================================

# Query 8: Retrieve the ST-001 observation from the baseline
# Silver snapshot saved before the changes batch.
spark.sql(f"""
    SELECT
        temperature_c,
        revision,
        event_id
    FROM local.weather_demo.silver_observations
    VERSION AS OF {silver_baseline_snapshot_id}
    WHERE station_id = 'ST-001'
      AND observed_at = TIMESTAMP '2024-06-15 12:00:00'
""").show(truncate=False)


# ============================================================
# 6. GOLD DAILY SUMMARY VERIFICATION
# ============================================================

# Query 9: Gold snapshot history
spark.sql("""
    SELECT
        committed_at,
        snapshot_id,
        operation
    FROM local.weather_demo.gold_daily_summary.snapshots
    ORDER BY committed_at
""").show(truncate=False)

# Query 10: Inspect daily Gold metrics for ST-001 and ST-003

spark.sql("""
WITH baseline_gold AS (
    SELECT
        station_id,
        observation_date,
        observation_count,
        avg_temperature_c,
        total_precipitation_mm
    FROM local.weather_demo.gold_daily_summary
    VERSION AS OF 1504273939043582168
    WHERE observation_date = DATE '2024-06-15'
      AND station_id IN ('ST-001', 'ST-003')
),
current_gold AS (
    SELECT
        station_id,
        observation_date,
        observation_count,
        avg_temperature_c,
        total_precipitation_mm
    FROM local.weather_demo.gold_daily_summary
    WHERE observation_date = DATE '2024-06-15'
      AND station_id IN ('ST-001', 'ST-003')
)
SELECT
    c.station_id,
    c.observation_date,
    b.observation_count AS old_count,
    c.observation_count AS current_count,
    b.avg_temperature_c AS old_avg_temperature,
    c.avg_temperature_c AS current_avg_temperature,
    b.total_precipitation_mm AS old_precipitation,
    c.total_precipitation_mm AS current_precipitation
FROM baseline_gold b
JOIN current_gold c
    ON b.station_id = c.station_id
   AND b.observation_date = c.observation_date
ORDER BY c.station_id
""").show(truncate=False)


# ============================================================
# END OF WEATHERLEDGER SQL VERIFICATION QUERIES
# ============================================================