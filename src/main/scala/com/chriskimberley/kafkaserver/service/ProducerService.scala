package com.chriskimberley.kafkaserver.service

import java.util.Properties

import scala.jdk.CollectionConverters.*

import zio.{RIO, Task, TaskLayer, ZIO, ZLayer}

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import com.chriskimberley.kafkaserver.config.KafkaConfig

trait ProducerService {
  def produce(producerRecord: ProducerRecord[String, String]): Task[Unit]
}

object ProducerService {
  def produce(producerRecord: ProducerRecord[String, String]): RIO[ProducerService, Unit] =
    ZIO.serviceWithZIO[ProducerService](_.produce(producerRecord))
}

case class ProducerServiceImpl(
  producer: KafkaProducer[String, String],
  config: KafkaConfig
) extends ProducerService {
  override def produce(producerRecord: ProducerRecord[String, String]): Task[Unit] =
    ZIO.attempt(producer.send(producerRecord).get())
}

object ProducerServiceImpl {
  private def setupTopic(config: KafkaConfig): Task[Unit] = ZIO.attempt {
    val adminProperties = new Properties()
    adminProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)

    val admin = AdminClient.create(adminProperties)
    try
      val newTopic = NewTopic(config.topicName, config.partitionCount, 1.toShort)
      admin.createTopics(List(newTopic).asJava).all().get()
    finally admin.close()
  }

  private def createProducerProperties(config: KafkaConfig): Properties = {
    val properties = new Properties()
    properties.put(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      config.bootstrapServers
    )
    properties.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    properties.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    properties
  }

  def layer(config: KafkaConfig): TaskLayer[ProducerServiceImpl] =
    ZLayer.scoped {
      for {
        _ <- setupTopic(config).orDie
        producer <- ZIO.acquireRelease(
                      ZIO.attempt(KafkaProducer[String, String](createProducerProperties(config)))
                    )(p => ZIO.attempt(p.close()).ignoreLogged)
      } yield ProducerServiceImpl(producer, config)
    }
}
