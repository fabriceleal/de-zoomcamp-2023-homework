
# Question 1

Files:
[schema](staging/schema.yml)
[stg_green_tripdata.sql](staging/stg_green_tripdata.sql)
[stg_yellow_tripdata.sql](staging/stg_yellow_tripdata.sql)
[fact_trips.sql](core/fact_trips.sql)

```
SELECT count(1) 
FROM `raptor-land.dbt_fleal.fact_trips` 
where 
  extract(year from  pickup_datetime) between 2019 and 2020
```

61641974

# Question 2

data studio

pie chart
data range dimension: pickpup_datetime
dimension: service type
metric: record count

94/6

# Question 3

[schema](staging/schema.yml)
[stg_fhv_tripdata.sql](staging/stg_fhv_tripdata.sql)
[fact_fhv_trips.sql](core/fact_fhv_trips.sql)

dbt run --select stg_fhv_tripdata --var 'is_test_run: false'

43244696
	
# Question 4

22998722

# Question 5

[fact_fhv_trips_report.pdf](fact_fhv_trips_report.pdf)

January
