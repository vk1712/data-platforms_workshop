/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: Trip start timestamp
    nullable: false
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Trip end timestamp
    nullable: false
    primary_key: true
    checks:
      - name: not_null
  - name: passenger_count
    type: int
    description: Number of passengers in the vehicle
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: trip_distance
    type: double
    description: Distance traveled in miles
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: pickup_location_id
    type: int
    description: TLC zone ID for pickup location
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: int
    description: TLC zone ID for dropoff location
    nullable: false
    checks:
      - name: not_null
  - name: fare_amount
    type: double
    description: Base fare amount
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: extra
    type: double
    description: Extra charges (rush hour, overnight)
    nullable: true
    checks:
      - name: non_negative
  - name: mta_tax
    type: double
    description: Metropolitan Transportation Authority tax
    nullable: true
    checks:
      - name: non_negative
  - name: tip_amount
    type: double
    description: Tip given to driver
    nullable: true
    checks:
      - name: non_negative
  - name: tolls_amount
    type: double
    description: Toll charges
    nullable: true
    checks:
      - name: non_negative
  - name: total_amount
    type: double
    description: Total trip amount including all charges and tips
    nullable: false
    checks:
      - name: not_null
      - name: non_negative
  - name: payment_type
    type: int
    description: Payment method (1=Card, 2=Cash, 3=No charge, 4=Dispute)
    nullable: false
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Human readable payment method name (lookup from ingestion.payment_lookup)
    nullable: true
  - name: trip_type
    type: int
    description: Trip type (1=Street hail, 2=Dispatch)
    nullable: false
    checks:
      - name: not_null
  - name: extracted_at
    type: timestamp
    description: The timestamp when the source data was extracted
    nullable: false
    checks:
      - name: not_null

custom_checks:
  - name: row_count_positive
    description: Ensure the staging table is not empty for the run window
    query: |
      -- TODO: return a single scalar (COUNT(*), etc.) that should match `value`
      SELECT COUNT(*) > 0 FROM staging.trips
    value: 1
  - name: unique_trip_key
    description: Ensure duplicate trip records are deduplicated within the staging window
    query: |
      SELECT COUNT(*) = COUNT(DISTINCT (pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount, total_amount, payment_type))
      FROM staging.trips
    value: 1

@bruin */

WITH raw AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY pickup_datetime,
                   dropoff_datetime,
                   pickup_location_id,
                   dropoff_location_id,
                   fare_amount,
                   total_amount,
                   payment_type
      ORDER BY extracted_at DESC
    ) AS _row_num
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
    AND pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND passenger_count > 0
    AND trip_distance >= 0
    AND fare_amount >= 0
    AND total_amount >= 0
)

SELECT
  r.pickup_datetime,
  r.dropoff_datetime,
  r.passenger_count,
  r.trip_distance,
  r.pickup_location_id,
  r.dropoff_location_id,
  r.fare_amount,
  r.extra,
  r.mta_tax,
  r.tip_amount,
  r.tolls_amount,
  r.total_amount,
  r.payment_type,
  pl.payment_type_name,
  r.trip_type,
  r.extracted_at
FROM raw r
LEFT JOIN ingestion.payment_lookup pl
  ON r.payment_type = pl.payment_type_id
WHERE r._row_num = 1
