package com.chriskimberley.kafkaserver.config

import java.time.Duration

case class KafkaConfig(
  topicName: String,
  bootstrapServers: String = "localhost:9092",
  partitionCount: Int = 3,
  replicationFactor: Short = 1,
  pollTimeout: Duration = Duration.ofMillis(100)
)
