package com.chriskimberley.kafkaserver.app

import zio.{Task, ZIO, ZIOAppDefault}

import com.chriskimberley.kafkaserver.config.KafkaConfig
import com.chriskimberley.kafkaserver.service.{
  ConsumerService,
  ConsumerServiceImpl,
  DataLoadService,
  DataLoadServiceImpl,
  ProducerService,
  ProducerServiceImpl
}

object Main extends ZIOAppDefault {
  override def run: Task[Seq[String]] = {
    val config = KafkaConfig(
      bootstrapServers = "localhost:9092",
      topicName = "test-topic11"
    )

    val program = for
      producerRecords <- DataLoadService.constructProducerRecordsFromFile
      _ <- ZIO.foreachParDiscard(producerRecords)(ProducerService.produce)
      messages <- ConsumerService.consume(50, 10)
      _ <- ZIO.foreachDiscard(messages)(message => ZIO.succeed(println(message)))
    yield messages

    program.provide(
      DataLoadServiceImpl.layer(config),
      ProducerServiceImpl.layer(config),
      ConsumerServiceImpl.layer(config)
    )
  }
}
