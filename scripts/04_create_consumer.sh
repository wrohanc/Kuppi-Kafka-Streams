export KAFKA_HOME=/Users/rohan/Documents/servers/kafka_2.12-2.6.1
cd $KAFKA_HOME
bin/kafka-console-consumer.sh --topic delivery-orders --from-beginning --bootstrap-server localhost:9092