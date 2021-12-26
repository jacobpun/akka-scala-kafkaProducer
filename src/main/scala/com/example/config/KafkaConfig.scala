package com.example.config

object KafkaConfig {
  val applicationID = "ExampleProducer";
  val bootstrapServers = "localhost:9092,localhost:9093";
  val topicName = "file-content-topic";
}