export KAFKA_HOME=/Users/rohan/Documents/servers/kafka_2.12-2.6.1
cd $KAFKA_HOME
bin/kafka-console-producer.sh --topic web-orders-new --bootstrap-server localhost:9092