
"""
Prepared helper: generate deterministic synthetic weather input files.
Run before recording; this is not one of the three live-coded programs.
"""

import csv
import math
import os
import random
import sys
from datetime import datetime, timedelta

OUT = "data/weatherledger_project/landing"
BASELINE = os.path.join(OUT, "weather_baseline.csv")
CHANGES = os.path.join(OUT, "weather_changes.csv")

FIELDS = [
    "event_id",
    "station_id",
    "observed_at",
    "ingested_at",
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "wind_speed_kmh",
    "revision",
    "source_batch_id",
]

STATION_COUNT = 8
DEFAULT_YEAR = 2025


def choose_year():
    """Get the requested year from the command line or prompt."""
    if len(sys.argv) > 1:
        value = sys.argv[1].strip()
    else:
        value = input(
            f"Enter the year to generate weather data for [{DEFAULT_YEAR}]: "
        ).strip()

        if not value:
            value = str(DEFAULT_YEAR)

    try:
        year = int(value)
    except ValueError:
        raise SystemExit("Invalid year. Please enter a whole number, such as 2026.")

    # YEAR + 1 must be representable by datetime.
    if year < 1900 or year > 9998:
        raise SystemExit("Please enter a year between 1900 and 9998.")

    return year


YEAR = choose_year()

BASELINE_BATCH_ID = f"baseline-{YEAR}"
CHANGES_BATCH_ID = f"weather-changes-{YEAR}-10-01"

# Calculate the boundaries of the selected calendar year.
start = datetime(YEAR, 1, 1)
end = datetime(YEAR + 1, 1, 1)

days_in_year = (end - start).days
hours_in_year = int((end - start).total_seconds() // 3600)

# The same seed makes the generated data reproducible.
random.seed(27)
os.makedirs(OUT, exist_ok=True)

# ---------------------------------------------------------
# 1. Generate the baseline observations for the chosen year.
# ---------------------------------------------------------

baseline = {}

with open(BASELINE, "w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=FIELDS)
    writer.writeheader()

    for station_num in range(1, STATION_COUNT + 1):
        station = f"ST-{station_num:03d}"

        for hour in range(hours_in_year):
            observed = start + timedelta(hours=hour)

            # Annual and daily temperature patterns.
            seasonal = 9 * math.sin(
                2 * math.pi
                * (observed.timetuple().tm_yday - 80)
                / days_in_year
            )

            daily = 5 * math.sin(
                2 * math.pi * (observed.hour - 6) / 24
            )

            temp = round(
                13
                + station_num * 0.7
                + seasonal
                + daily
                + random.uniform(-2, 2),
                2,
            )

            row = {
                "event_id": (
                    f"evt-{station}-{observed:%Y%m%dT%H%M%S}-r1"
                ),
                "station_id": station,
                "observed_at": observed.strftime("%Y-%m-%d %H:%M:%S"),
                "ingested_at": (
                    observed + timedelta(minutes=2)
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "temperature_c": temp,
                "humidity_pct": round(random.uniform(25, 95), 2),
                "precipitation_mm": round(
                    random.choice(
                        [0, 0, 0, 0, random.uniform(0.1, 5)]
                    ),
                    2,
                ),
                "wind_speed_kmh": round(random.uniform(0, 45), 2),
                "revision": 1,
                "source_batch_id": BASELINE_BATCH_ID,
            }

            writer.writerow(row)
            baseline[(station, observed)] = row

# ---------------------------------------------------------
# 2. Generate correction, late-arriving, and duplicate events.
# ---------------------------------------------------------

changes = []

# All scenario dates are based on the selected year.
changes_ingested_at = datetime(YEAR, 10, 1, 10, 0, 0)

# Scenario A: correction of an existing observation.
correction_time = datetime(YEAR, 6, 15, 12, 0, 0)
key = ("ST-001", correction_time)

corrected = baseline[key].copy()
corrected.update(
    event_id=f"evt-ST-001-{correction_time:%Y%m%dT%H%M%S}-r2",
    ingested_at=changes_ingested_at.strftime("%Y-%m-%d %H:%M:%S"),
    temperature_c=round(
        float(corrected["temperature_c"]) + 2.5, 2
    ),
    revision=2,
    source_batch_id=CHANGES_BATCH_ID,
)
changes.append(corrected)

# Scenario B: a late-arriving observation at a new timestamp.
late_time = datetime(YEAR, 6, 15, 12, 30, 0)

changes.append(
    {
        "event_id": f"evt-ST-003-{late_time:%Y%m%dT%H%M%S}-r1",
        "station_id": "ST-003",
        "observed_at": late_time.strftime("%Y-%m-%d %H:%M:%S"),
        "ingested_at": (
            changes_ingested_at + timedelta(minutes=5)
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "temperature_c": 21.5,
        "humidity_pct": 63.0,
        "precipitation_mm": 0.2,
        "wind_speed_kmh": 12.0,
        "revision": 1,
        "source_batch_id": CHANGES_BATCH_ID,
    }
)

# Scenario C: the same observation delivered twice.
duplicate_time = datetime(YEAR, 6, 20, 8, 0, 0)
key = ("ST-002", duplicate_time)

for label, minutes_later in [
    ("delivery-a", 6),
    ("delivery-b", 7),
]:
    duplicate = baseline[key].copy()

    duplicate.update(
        event_id=(
            f"evt-ST-002-{duplicate_time:%Y%m%dT%H%M%S}-{label}"
        ),
        ingested_at=(
            changes_ingested_at + timedelta(minutes=minutes_later)
        ).strftime("%Y-%m-%d %H:%M:%S"),
        source_batch_id=CHANGES_BATCH_ID,
    )

    changes.append(duplicate)

with open(CHANGES, "w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(changes)

baseline_rows = STATION_COUNT * hours_in_year

print()
print(f"Generated weather data for {YEAR}")
print(f"Calendar days: {days_in_year}")
print(f"Stations: {STATION_COUNT}")
print(f"Baseline: {BASELINE} ({baseline_rows:,} rows)")
print(
    f"Changes:  {CHANGES} ({len(changes)} rows: "
    "correction, late event, duplicate deliveries)"
)
print(f"Baseline batch ID: {BASELINE_BATCH_ID}")
print(f"Changes batch ID:  {CHANGES_BATCH_ID}")
print("All values are synthetic; no real weather API or third-party dataset was used.")
