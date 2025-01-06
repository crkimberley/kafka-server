ThisBuild / scalaVersion := "3.6.2"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.chriskimberley.kafkaserver"
ThisBuild / organizationName := "example"

lazy val zioVersion = "2.1.6"
lazy val zioKafkaVersion = "2.8.0"
lazy val zioJsonVersion = "0.6.2"
val zioHttpVersion = "3.0.1"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-server",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,
      "dev.zio" %% "zio-kafka" % zioKafkaVersion,
      "dev.zio" %% "zio-json" % zioJsonVersion,
      "dev.zio" %% "zio-http" % zioHttpVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
