export KAFKA_BIN=/Users/rohan/Documents/servers/kafka_2.12-2.6.1/bin
export PATH=$KAFKA_BIN:$PATH
#kafka-topics.sh --create --topic web-orders-new --bootstrap-server localhost:9092 --partitions 5 --replication-factor 3
#kafka-topics.sh --create --topic web-orders-validated --bootstrap-server localhost:9092 --partitions 5 --replication-factor 3
#kafka-topics.sh --create --topic web-orders-invalidated --bootstrap-server localhost:9092 --partitions 5 --replication-factor 3
kafka-topics.sh --create --topic pickup-orders --bootstrap-server localhost:9092 --partitions 5 --replication-factor 3
kafka-topics.sh --create --topic delivery-orders --bootstrap-server localhost:9092 --partitions 5 --replication-factor 3