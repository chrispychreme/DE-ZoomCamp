{{
    config(
        materialized='view'
    )
}}

select
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pulocationid,
    dolocationid,
    sr_flag,
    affiliated_base_number
from
    {{ source('staging','fhv_tripdata') }}
where
    1 = 1
    and EXTRACT(YEAR FROM pickup_datetime) = 2019

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}