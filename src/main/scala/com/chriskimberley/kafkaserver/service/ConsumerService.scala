package com.chriskimberley.kafkaserver.service

import java.util.Properties

import scala.jdk.CollectionConverters.*

import zio.{RIO, Task, TaskLayer, ZIO, ZLayer}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import com.chriskimberley.kafkaserver.config.KafkaConfig

trait ConsumerService {
  def consume(offset: Long, count: Int): Task[Seq[String]]
}

object ConsumerService {
  def consume(offset: Long, count: Int): RIO[ConsumerService, Seq[String]] =
    ZIO.serviceWithZIO[ConsumerService](_.consume(offset, count))
}

case class ConsumerServiceImpl(
  consumer: KafkaConsumer[String, String],
  config: KafkaConfig
) extends ConsumerService {
  override def consume(offset: Long, count: Int): Task[Seq[String]] =
    ZIO.attempt {
      val partitions = (0 until config.partitionCount)
        .map(TopicPartition(config.topicName, _))
      consumer.assign(partitions.asJava)

      partitions.foreach(consumer.seek(_, offset))

      consumer
        .poll(config.pollTimeout)
        .iterator()
        .asScala
        .toSeq
        .map(_.value())
        .take(count)
    }
}

object ConsumerServiceImpl {
  private def createConsumerProperties(config: KafkaConfig): Properties = {
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
    properties.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    properties.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties
  }

  def layer(config: KafkaConfig): TaskLayer[ConsumerServiceImpl] =
    ZLayer.scoped {
      for {
        consumer <-
          ZIO.acquireRelease(
            ZIO.attempt(new KafkaConsumer[String, String](createConsumerProperties(config)))
          )(c => ZIO.attempt(c.close()).ignoreLogged)
      } yield ConsumerServiceImpl(consumer, config)
    }
}
