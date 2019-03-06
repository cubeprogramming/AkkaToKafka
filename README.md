# AkkaToKafka

This is a Demo project which purpose is to demonstrate Akka Actor instantiation and message sending to Kafka

## Code description

1. Main class reads the list of CSV files and instantiate two Actor objects for each CSF file in the list
2. Message is sent to first set of Actors containing the file to be processed (FileProcessor)
3. File is read and each line is send to second set of Actors (KafkaWriter)
4. Each KafkaWriter actor is than converting CSV line to JSON format 
5. Line is written as a JSON snippet to Kafka topic
