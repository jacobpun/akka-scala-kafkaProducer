kafka-topics.sh --create --zookeeper localhost:2181 --topic file-content-topic --partitions 5 --replication-factor 3 --config segment.bytes=1000000
