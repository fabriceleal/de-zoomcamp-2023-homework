# Question 1

All are correct

# Question 2

Topic replication, Ack all

# Question 3

Topic partitioning, consumer group id

# Question 4

due to their low cardinality, the following are good candidates

payment_type, vendor_id, passenger_count

we may extract month to partition by date fields, but we shouldnt use them as is

# Question 5

deserializer configuration, topic subscription, group id, offset

# Question 6

[producer.py](hw_week6/producer.py)
[consumer.py](hw_week6/consumer.py)
