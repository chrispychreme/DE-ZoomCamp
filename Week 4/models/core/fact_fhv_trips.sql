{{
    config(
        materialized='table'
    )
}}

with
    dim_zones as (
        select * from {{ ref('dim_zones') }}
        where borough != 'Unknown'
    )

select
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pulocationid,
    dolocationid,
    sr_flag,
    affiliated_base_number
from
    {{ ref('stg_fhv_tripdata') }} as fhv
    join dim_zones as dmpu
        on fhv.pulocationid = dmpu.locationid
    join dim_zones as dmdo
        on fhv.dolocationid = dmdo.locationid