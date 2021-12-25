name := "akka-quickstart-scala"

version := "1.0"

scalaVersion := "2.13.7"

lazy val akkaVersion = "2.6.18"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "org.apache.kafka" % "kafka-clients" % "2.3.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.1.0" % Test
)
