package com.chriskimberley.kafkaserver.http

import zio.http.*
import zio.http.Method.GET
import zio.json.*
import zio.json.ast.Json
import zio.{URIO, ZIO}

import com.chriskimberley.kafkaserver.service.ConsumerService

object KafkaServer {
  private val DefaultOffset = 0
  private val DefaultCount = 10

  val routes: Routes[ConsumerService, Nothing] = Routes(
    GET / "topic" / string("topicName") / long("offset") ->
      handler { (topicName: String, offset: Long, req: Request) =>
        getKafkaMessages(req, topicName, offset)
      },
    GET / "topic" / string("topicName") ->
      handler { (topicName: String, req: Request) =>
        getKafkaMessages(req, topicName, DefaultOffset)
      }
  )

  private def getKafkaMessages(
    req: Request,
    topicName: String,
    offset: Long
  ): URIO[ConsumerService, Response] = {
    val count = req.queryParamToOrElse("count", DefaultCount)
    ZIO.logInfo(s"Request for $count messages, offset: $offset, topic: $topicName") *>
      ConsumerService
        .consume(offset, count)
        .map(messages => Response.json(s"""[
           |  ${messages.map(str => s""""$str"""").mkString(",\n\n  ")}
           |]""".stripMargin))
        .catchAll(error =>
          ZIO.succeed(
            Response
              .json(Json.Obj(("error", Json.Str(error.getMessage))).toString())
              .status(Status.InternalServerError)
          )
        )
  }
}
