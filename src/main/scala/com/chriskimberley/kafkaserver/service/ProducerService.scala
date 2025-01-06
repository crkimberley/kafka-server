package com.chriskimberley.kafkaserver.service

import java.util.Properties

import scala.jdk.CollectionConverters.*

import zio.{RIO, Task, TaskLayer, ZIO, ZLayer}

import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import com.chriskimberley.kafkaserver.config.KafkaConfig

trait ProducerService {
  def produce(producerRecord: ProducerRecord[String, String]): Task[Unit]
  def setupTopic: Task[Boolean]
}

object ProducerService {
  def produce(producerRecord: ProducerRecord[String, String]): RIO[ProducerService, Unit] =
    ZIO.serviceWithZIO[ProducerService](_.produce(producerRecord))

  def setupTopic: RIO[ProducerService, Boolean] =
    ZIO.serviceWithZIO[ProducerService](_.setupTopic)
}

case class ProducerServiceImpl(
  producer: KafkaProducer[String, String],
  config: KafkaConfig
) extends ProducerService {
  override def produce(producerRecord: ProducerRecord[String, String]): Task[Unit] =
    ZIO.attempt(producer.send(producerRecord).get())

  override def setupTopic: Task[Boolean] = {
    def createAdminClient = ZIO.attempt {
      val adminProperties = new Properties()
      adminProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
      AdminClient.create(adminProperties)
    }

    def topicExists(admin: AdminClient, topicName: String) = ZIO.attempt {
      admin.listTopics().names().get().asScala.contains(topicName)
    }

    def createTopic(admin: AdminClient) = ZIO.attempt {
      val newTopic = NewTopic(
        config.topicName,
        config.partitionCount,
        config.replicationFactor
      )
      admin.createTopics(List(newTopic).asJava).all().get()
    }

    ZIO.scoped {
      ZIO
        .acquireRelease(createAdminClient)(admin => ZIO.attempt(admin.close()).ignoreLogged)
        .flatMap { admin =>
          for {
            exists <- topicExists(admin, config.topicName)
            _ <- ZIO.unless(exists)(createTopic(admin))
          } yield exists
        }
    }
  }
}

object ProducerServiceImpl {
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
        producer <- ZIO.acquireRelease(
                      ZIO.attempt(KafkaProducer[String, String](createProducerProperties(config)))
                    )(p => ZIO.attempt(p.close()).ignoreLogged)
      } yield ProducerServiceImpl(producer, config)
    }
}
