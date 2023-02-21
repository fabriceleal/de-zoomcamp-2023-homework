{{ config(materialized='view') }}

select 
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    sr_flag,
    affiliated_base_number	
from {{ source('staging', 'fvh_data') }}
where 
    extract(year from pickup_datetime) = 2019

{% if var('is_test_run', default=true  )%}

limit 100

{% endif %}

