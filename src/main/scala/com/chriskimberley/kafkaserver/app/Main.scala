package com.chriskimberley.kafkaserver.app

import zio.http.Server
import zio.{Task, ZIO, ZIOAppDefault}

import com.chriskimberley.kafkaserver.config.KafkaConfig
import com.chriskimberley.kafkaserver.http.KafkaServer.routes
import com.chriskimberley.kafkaserver.service.{
  ConsumerService,
  ConsumerServiceImpl,
  DataLoadService,
  DataLoadServiceImpl,
  ProducerService,
  ProducerServiceImpl
}

object Main extends ZIOAppDefault {
  val Config = KafkaConfig(topicName = "test-topic-01")

  override def run: Task[Unit] = {
    val program = for {
      topicAlreadyExists <- ProducerService.setupTopic
      _ <- if !topicAlreadyExists then
             for {
               producerRecords <- DataLoadService.constructProducerRecordsFromFile
               _ <- ZIO.foreachParDiscard(producerRecords)(ProducerService.produce)
               _ <- ZIO.logInfo(s"Initial data loaded to Kafka for topic ${Config.topicName}")
             } yield ()
           else ZIO.logInfo(s"Topic ${Config.topicName} already exists, so no data is loaded")
      _ <- Server.serve(routes)
    } yield ()

    program.provide(
      DataLoadServiceImpl.layer(Config),
      ProducerServiceImpl.layer(Config),
      ConsumerServiceImpl.layer(Config),
      Server.default
    )
  }
}
