CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-data-412619.nyc_green_taxi_2022.fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ny-green-taxi-data/nyc_green_taxi_data_2022.parquet']
);

select distinct PULocationID from nyc_green_taxi_2022.data;
select distinct PULocationID from nyc_green_taxi_2022.fhv_tripdata;

select count(*) from nyc_green_taxi_2022.data where fare_amount = 0;

select datetime(timestamp_micros(cast(lpep_pickup_datetime / 1000 as int64))) from nyc_green_taxi_2022.fhv_tripdata limit 10;

SELECT count(*) FROM `ny-taxi-data-412619.nyc_green_taxi_2022.fhv_tripdata`;

SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `ny-taxi-data-412619.nyc_green_taxi_2022.fhv_tripdata`;

CREATE OR REPLACE TABLE `ny-taxi-data-412619.nyc_green_taxi_2022.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `ny-taxi-data-412619.nyc_green_taxi_2022.fhv_tripdata`;

ALTER TABLE `ny-taxi-data-412619.nyc_green_taxi_2022.fhv_nonpartitioned_tripdata`
ADD COLUMN new_lpep_pickup_datetime DATETIME;

ALTER TABLE nyc_green_taxi_2022.data
ADD COLUMN new_lpep_pickup_datetime DATETIME;

UPDATE `ny-taxi-data-412619.nyc_green_taxi_2022.fhv_nonpartitioned_tripdata`
SET new_lpep_pickup_datetime = datetime(timestamp_micros(cast(lpep_pickup_datetime / 1000 as int64)))
WHERE 1=1;

UPDATE nyc_green_taxi_2022.data
SET new_lpep_pickup_datetime = datetime(timestamp_micros(cast(lpep_pickup_datetime / 1000 as int64)))
WHERE 1=1;

CREATE OR REPLACE TABLE `ny-taxi-data-412619.nyc_green_taxi_2022.fhv_partitioned_tripdata`
PARTITION BY DATE(new_lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
  SELECT * FROM `ny-taxi-data-412619.nyc_green_taxi_2022.fhv_nonpartitioned_tripdata`
);

select distinct pulocationid from nyc_green_taxi_2022.data where cast(new_lpep_pickup_datetime as date) between '2022-06-01' and '2022-06-30';
select distinct pulocationid from nyc_green_taxi_2022.fhv_partitioned_tripdata where date(new_lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';