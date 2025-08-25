#!/bin/bash

# Delete the topics
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --delete --topic sms-in
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --delete --topic sms-out

# Recreate the topics
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --topic sms-in --replication-factor 1 --partitions 1
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --topic sms-out --replication-factor 1 --partitions 1
