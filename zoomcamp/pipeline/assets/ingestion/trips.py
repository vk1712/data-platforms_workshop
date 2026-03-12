"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: latest

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

description: |
  Fetch NYC Taxi trip data from the TLC public endpoint.
  Ingests raw trip records for the specified taxi types and date range.
  Uses append strategy for raw ingestion; duplicates are handled downstream.

columns:
  - name: vendor_id
    type: int
    description: Taxi vendor ID (1=Yellow Cab, 2=Medallion)
  - name: pickup_datetime
    type: timestamp
    description: Date and time when the trip started
  - name: dropoff_datetime
    type: timestamp
    description: Date and time when the trip ended
  - name: passenger_count
    type: int
    description: Number of passengers in the vehicle
  - name: trip_distance
    type: double
    description: Distance traveled in miles
  - name: pickup_location_id
    type: int
    description: TLC taxi zone ID for pickup location
  - name: dropoff_location_id
    type: int
    description: TLC taxi zone ID for dropoff location
  - name: fare_amount
    type: double
    description: Base fare amount
  - name: extra
    type: double
    description: Extra charges (rush hour, overnight)
  - name: mta_tax
    type: double
    description: Metropolitan Transportation Authority tax
  - name: tip_amount
    type: double
    description: Tip given to driver
  - name: tolls_amount
    type: double
    description: Toll charges
  - name: total_amount
    type: double
    description: Total trip amount including all charges and tips
  - name: payment_type
    type: int
    description: Payment method (1=Card, 2=Cash, 3=No charge, 4=Dispute)
  - name: trip_type
    type: int
    description: Trip type (1=Street hail, 2=Dispatch)
  - name: extracted_at
    type: timestamp
    description: UTC timestamp when the data was extracted

@bruin"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
import io


def materialize():
    """
    Fetch NYC taxi trip data from TLC and materialize into the ingestion table.

    Uses:
    - BRUIN_START_DATE / BRUIN_END_DATE: Time window for fetching data
    - BRUIN_VARS: Pipeline variables like taxi_types
    """
    # Get Bruin runtime context
    start_date = os.getenv("BRUIN_START_DATE", "2022-01-01")
    end_date = os.getenv("BRUIN_END_DATE", "2022-01-31")

    # Parse pipeline variables
    bruin_vars = os.getenv("BRUIN_VARS", "{}")
    vars_dict = json.loads(bruin_vars)
    taxi_types = vars_dict.get("taxi_types", ["yellow"])

    print(
        f"Ingesting trips from {start_date} to {end_date} for taxi types: {taxi_types}")

    # Generate list of source endpoints (one per taxi type per month)
    # Format: https://d37cirlwqrbdgo.cloudfront.net/csv_zip/yellow_tripdata_2022-01.parquet
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    # Generate all months between start and end using period_range
    months = pd.period_range(start=start.to_period('M'),
                             end=end.to_period('M'),
                             freq='M')

    print(f"Months to fetch: {[str(m) for m in months]}")

    dataframes = []

    for taxi_type in taxi_types:
        for month in months:
            year_month = str(month)  # Period objects convert to YYYY-MM format
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year_month}.parquet"

            try:
                print(f"Fetching {taxi_type} trips for {year_month}...")
                df = pd.read_parquet(url)

                # Standardize column names (handle both yellow and green taxi variations)
                rename_map = {
                    "tpep_pickup_datetime": "pickup_datetime",
                    "tpep_dropoff_datetime": "dropoff_datetime",
                    "lpep_pickup_datetime": "pickup_datetime",
                    "lpep_dropoff_datetime": "dropoff_datetime",
                    "PULocationID": "pickup_location_id",
                    "DOLocationID": "dropoff_location_id",
                    "Trip_type": "trip_type"
                }
                df = df.rename(columns=rename_map)

                # Ensure required columns exist
                required_cols = [
                    "vendor_id", "pickup_datetime", "dropoff_datetime",
                    "passenger_count", "trip_distance", "pickup_location_id",
                    "dropoff_location_id", "fare_amount", "extra", "mta_tax",
                    "tip_amount", "tolls_amount", "total_amount", "payment_type"
                ]

                # Fill missing columns with None
                for col in required_cols:
                    if col not in df.columns:
                        df[col] = None

                # Ensure trip_type exists
                if "trip_type" not in df.columns:
                    df["trip_type"] = 1  # Default value for trip records

                # Add extraction timestamp
                df["extracted_at"] = datetime.utcnow()

                # Select final columns in order
                final_cols = required_cols + ["trip_type", "extracted_at"]
                df = df[final_cols]

                dataframes.append(df)
                print(f"  ✓ Loaded {len(df)} records")

            except Exception as e:
                print(
                    f"  ⚠ Failed to fetch {taxi_type} trips for {year_month}: {str(e)}")
                continue

    if not dataframes:
        print("WARNING: No data was fetched. Returning empty DataFrame.")
        return pd.DataFrame(columns=[
            "vendor_id", "pickup_datetime", "dropoff_datetime",
            "passenger_count", "trip_distance", "pickup_location_id",
            "dropoff_location_id", "fare_amount", "extra", "mta_tax",
            "tip_amount", "tolls_amount", "total_amount", "payment_type",
            "trip_type", "extracted_at"
        ])

    # Concatenate all dataframes
    result_df = pd.concat(dataframes, ignore_index=True)

    print(f"✓ Ingestion complete: {len(result_df)} total trip records")
    return result_df
