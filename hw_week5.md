# Question 1

```
    import pyspark
    pyspark.__version__
```

Answer: 3.3.2

# Question 2

```
from pyspark.sql import types

schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropoff_datetime', types.TimestampType(), True), 
    types.StructField('PULocationID', types.IntegerType(), True), 
    types.StructField('DOLocationID', types.IntegerType(), True), 
    types.StructField('SR_Flag', types.StringType(), True), 
    types.StructField('Affiliated_base_number', types.StringType(), True)
])

df = spark.read.option('header', 'true').schema(schema).csv('fhvhv_tripdata_2021-06.csv')
df = df.repartition(12)
df.write.parquet('fhvhv/2021/06')

```


Average size: 23M

# Question 3

```
df = spark.read.parquet('fhvhv/2021/06')
df_dateinfo = df.withColumn('pickup_year', F.year(df.pickup_datetime)) \
   .withColumn('pickup_month', F.month(df.pickup_datetime))  \
   .withColumn('pickup_day', F.dayofmonth(df.pickup_datetime))

df_dateinfo.filter((df_dateinfo.pickup_day == 15) & (df_dateinfo.pickup_month == 6) & (df_dateinfo.pickup_year ==  2021)).count()

```

Answer: 452470


# Question 4

```
    df.registerTempTable("df")
    spark.sql("""
        select duration, dropoff_datetime, pickup_datetime
        from (
            select 
                (bigint(to_timestamp(dropoff_datetime)) - bigint(to_timestamp(pickup_datetime))) / 3600  AS duration,
                dropoff_datetime, pickup_datetime
            from df    
        )
        order by duration desc
        limit 1
    """).show()
```

Answer: 66.87

# Question 5

Answer: 4040

# Question 6

```
    df_zones = spark.read.option('header', 'true').csv('taxi_zone_lookup.csv')
    df_zones.registerTempTable("df_zones")

    spark.sql("""

    select df_zones.*, aux.pickups
    from 
    (select df.PULocationID, count(1) as pickups
    from df 
    group by df.PULocationID) aux inner join df_zones on aux.PULocationID = df_zones.LocationID
    order by aux.pickups desc
    limit 1

    """).show()
```

Answer: Crown Heights North 
