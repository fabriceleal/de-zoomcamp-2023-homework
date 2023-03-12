import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1 pyspark-shell'

from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .appName("Spark-Notebook") \
    .getOrCreate()

class SparkConsumer():
        
    def get_topic(self, topic):
        return spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("checkpointLocation", "checkpoint") \
            .load().selectExpr("CAST(key AS STRING)","CAST(value AS STRING)", "CAST(topic as STRING)")
    
    def sink_console(self, df, output_mode: str = 'append', processing_time: str = '5 seconds'):
            write_query = df.writeStream \
                .outputMode(output_mode) \
                .trigger(processingTime=processing_time) \
                .format("console") \
                .option("truncate", False) \
                .start()
            return write_query
    
    def count_by_key(self, topic):
        df_ag = self.get_topic(topic).groupBy("key").count()
        df_ag_sorted = df_ag.filter(df_ag["key"] != "0").orderBy(df_ag["count"].desc())
        self.sink_console(df_ag_sorted, output_mode="complete")
        df = self.prepare_dataframe_to_kafka_sink(df_ag_sorted, ["count"])
        self.sink_kafka(df, "rides_aggreg", "complete")
        
    def prepare_dataframe_to_kafka_sink(self, df, value_columns, key_column=None):
        columns = df.columns
        df = df.withColumn("value", F.concat_ws(', ',*value_columns))    
        if key_column:
            df = df.withColumnRenamed(key_column,"key")
            df = df.withColumn("key",df.key.cast('string'))
        return df.select(['key', 'value'])

    def sink_kafka(self, df, topic, output_mode='append'):
        write_query = df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
            .outputMode(output_mode) \
            .option("topic", topic) \
            .option("checkpointLocation", "checkpoint") \
            .start()
        return write_query

    def sink_to_rides_all(self):
        df = self.prepare_dataframe_to_kafka_sink(self.get_topic("rides_green"), ["value"])
        self.sink_kafka(df, "rides_all")
        df2 = self.prepare_dataframe_to_kafka_sink(self.get_topic("rides_fhv"), ["value"])
        self.sink_kafka(df2, "rides_all")

s = SparkConsumer()

if __name__ == "__main__":
    s.count_by_key("rides_all")
    s.sink_to_rides_all()

    spark.streams.awaitAnyTermination()
