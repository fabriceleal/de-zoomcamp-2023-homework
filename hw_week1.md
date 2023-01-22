# homework week 1

## question 1

Run: `docker build --help`

Answer: Option `--iidfile string`

## question 2

Run: `docker run -it --entrypoint=bash python:3.9`
and `pip list` in the bash repl

Answer: 3 packages

```
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4
```

## question 3

setup as in https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=13
with green_taxi_data

Connect to postgres with `pgcli -h localhost -U root -d ny_taxi`

run sql query: 
```
select count(1) 
from green_taxi_data 
where   date(lpep_pickup_datetime) = '2019-01-15' 
    and date(lpep_dropoff_datetime) = '2019-01-15' 
```

Answer: 20530

## question 4

run sql query:
```
select date(lpep_pickup_datetime) from green_taxi_data where trip_distance = 
 (select max(trip_distance)from green_taxi_data)
```

Answer: 2019-01-15

## question 5

run the 2 sql queries:
```
select count(1) from green_taxi_data where passenger_count = 2 and 
date(lpep_pickup_datetime) = '2019-01-01'

select count(1) from green_taxi_data where passenger_count = 3 and 
date(lpep_pickup_datetime) = '2019-01-01'
```

Answer: 2: 1282 ; 3: 254

## question 6

run sql query:

```
select "Zone" 
from 
    green_taxi_data inner join zones 
    on green_taxi_data."DOLocationID" = zones."LocationID" 
where tip_amount = 
    (select max(tip_amount) 
    from green_taxi_data 
    where "PULocationID" = 
        (select "LocationID" from zones where "Zone" = 'Astoria'))

```

Answer: Long Island City/Queens Plaza

