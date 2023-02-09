
# Question 1

Download fvh data and upload to Google Cloud Storage with [push_fvh2gcloud.py](hw_week3/push_fvh2gcloud.py)


Create external table:
```
CREATE OR REPLACE EXTERNAL TABLE `raptor-land.de_zoomcamp.external_fhv_data`
OPTIONS (
	 format= 'CSV',
	  uris = ['gs://dtc_data_lake_raptor-land/fhv2019/fhv_tripdata_2019-*.csv.gz']
);
```
Create materialized table from external table:
```
CREATE OR REPLACE TABLE `raptor-land.de_zoomcamp.fhv_data`
as
select * from raptor-land.de_zoomcamp.external_fhv_data;
```

Count rows in table
```
select count(1) from raptor-land.de_zoomcamp.fhv_data;
```

Rows: 43244696


# Question 2

```
select count(affiliated_base_number) from raptor-land.de_zoomcamp.external_fhv_data;

select count(affiliated_base_number) from raptor-land.de_zoomcamp.fhv_data;
```

external: 0 MB
bigtable: 317.94 MB 

# Question 3

```
select count(1) 
from raptor-land.de_zoomcamp.fhv_data
where PUlocationID is null and DOlocationID is null
```

Rows: 717748
	
# Question 4

Partition by pickup_datetime 
Cluster on affiliated_base_number

# Question 5

```
CREATE OR REPLACE TABLE `raptor-land.de_zoomcamp.fhv_data_question5`
partition by date(pickup_datetime)
cluster by affiliated_base_number  
as
select *
from raptor-land.de_zoomcamp.fhv_data;

select count(affiliated_base_number)
from raptor-land.de_zoomcamp.fhv_data
where 
    date(pickup_datetime) between '2019-03-01' and '2019-03-31'

```
(will process 647.87MB)

```
select count(affiliated_base_number)
from raptor-land.de_zoomcamp.fhv_data_question5
where 
    date(pickup_datetime) between '2019-03-01' and '2019-03-31'
``` 
(will process 23.05MB)

# Question 6

GCP Bucket

# Question 7

False

# Question 8

Check [parquetflow.py](hw_week3/parquetflow.py)
