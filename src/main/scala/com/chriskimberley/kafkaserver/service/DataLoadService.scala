package com.chriskimberley.kafkaserver.service

import java.nio.file.Paths

import zio.*
import zio.json.*
import zio.json.ast.Json

import org.apache.kafka.clients.producer.ProducerRecord

import com.chriskimberley.kafkaserver.config.KafkaConfig
import com.chriskimberley.kafkaserver.service.DataLoadServiceImpl.{KeyField, PeopleArrayName, PeopleDataFilePath}

trait DataLoadService {
  def constructProducerRecordsFromFile: Task[Seq[ProducerRecord[String, String]]]
}

object DataLoadService {
  def constructProducerRecordsFromFile: RIO[DataLoadService, Seq[ProducerRecord[String, String]]] =
    ZIO.serviceWithZIO[DataLoadService](_.constructProducerRecordsFromFile)
}

case class DataLoadServiceImpl(config: KafkaConfig) extends DataLoadService {
  private def readJsonObjectsFromFile: Task[Seq[Json]] =
    for {
      fileContentAsString <- ZIO.readFile(Paths.get(PeopleDataFilePath))
      json <- ZIO
                .fromEither(fileContentAsString.fromJson[Json])
                .orElseFail(new RuntimeException("Failed to parse JSON file"))
      jsons <- ZIO
                 .fromOption(
                   json.asObject
                     .flatMap(_.get(PeopleArrayName))
                     .flatMap(_.as[Seq[Json]].toOption)
                 )
                 .orElseFail(
                   new RuntimeException(s"$PeopleArrayName field not found or invalid")
                 )
    } yield jsons

  private def extractKey(json: Json): Task[String] =
    ZIO
      .fromOption(
        json.asObject
          .flatMap(_.get(KeyField))
          .flatMap(_.as[String].toOption)
      )
      .orElseFail(new RuntimeException(s"Key field '$KeyField' not found"))

  def constructProducerRecordsFromFile: Task[Seq[ProducerRecord[String, String]]] =
    for {
      jsons <- readJsonObjectsFromFile
      producerRecords <- ZIO.foreach(jsons) { json =>
                           extractKey(json)
                             .map(ProducerRecord[String, String](config.topicName, _, json.toString()))
                         }
      _ <- ZIO.logInfo(s"${producerRecords.size} ProducerRecords constructed from local file")
    } yield producerRecords
}

object DataLoadServiceImpl {
  private val PeopleDataFilePath = "random-people-data.json"
  private val PeopleArrayName = "ctRoot"
  private val KeyField = "_id"

  def layer(config: KafkaConfig): TaskLayer[DataLoadServiceImpl] =
    ZLayer.succeed(DataLoadServiceImpl(config))
}
