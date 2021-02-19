export KAFKA_BIN=/Users/rohan/Documents/servers/kafka_2.12-2.6.1/bin
export PATH=$KAFKA_BIN:$PATH
kafka-console-producer.sh --topic web-orders-validated --bootstrap-server localhost:9092