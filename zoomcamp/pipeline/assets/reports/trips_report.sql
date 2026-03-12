/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: report_date
  time_granularity: date

columns:
  - name: report_date
    type: date
    description: Trip start date (derived from pickup_datetime)
    primary_key: true
  - name: payment_type_name
    type: string
    description: Human readable payment method name
    primary_key: true
  - name: trip_type
    type: int
    description: Trip type (1=Street hail, 2=Dispatch)
    primary_key: true
  - name: total_trips
    type: bigint
    description: Number of trips
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: double
    description: Total fare amount for the group
    checks:
      - name: non_negative
  - name: total_tip_amount
    type: double
    description: Total tip amount for the group
    checks:
      - name: non_negative
  - name: avg_fare_amount
    type: double
    description: Average fare amount per trip
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  CAST(pickup_datetime AS DATE) AS report_date,
  payment_type_name,
  trip_type,
  COUNT(*) AS total_trips,
  SUM(fare_amount) AS total_fare_amount,
  SUM(tip_amount) AS total_tip_amount,
  AVG(fare_amount) AS avg_fare_amount
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  payment_type_name,
  trip_type
