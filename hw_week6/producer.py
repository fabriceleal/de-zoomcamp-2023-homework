import csv
import json
from time import sleep
from typing import Dict
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = 'localhost:9092'

class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_records(resource_path: str, get_key, schema=None):
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                key = get_key(row)
                yield (key, json.dumps(row, default=str))

    def publish(self, topic: str, records):
        i = 0
        for key_value in records:
            try:
                key, value = key_value
                i = i + 1
                self.producer.send(topic=topic, key=key, value=value)
                if i % 100000 == 0:
                    print(f'{topic} Record {i}')
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)

def produce_file(filename, topic, get_key):
    config = {
        'bootstrap_servers': [BOOTSTRAP_SERVERS],
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8')
    }
    
    ride_records = RideCSVProducer.read_records(resource_path=filename, get_key=get_key)    
    producer = RideCSVProducer(props=config)    
    producer.publish(topic=topic, records=ride_records)

def clean_key(v):
    try:
        return str(int(v))
    except:
        return "0"

if __name__ == "__main__":    
    produce_file("/home/fabriceleal/data/fhv_tripdata_2019-01.csv", "rides_fhv", lambda row: clean_key(row[3]))
    produce_file("/home/fabriceleal/data/green_tripdata_2019-01.csv", "rides_green", lambda row: clean_key(row[5]))
