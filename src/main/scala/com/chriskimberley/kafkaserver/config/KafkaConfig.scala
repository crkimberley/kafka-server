package com.chriskimberley.kafkaserver.config

import java.time.Duration

case class KafkaConfig(
  bootstrapServers: String,
  topicName: String,
  partitionCount: Int = 3,
  pollTimeout: Duration = Duration.ofMillis(100)
)
