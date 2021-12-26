package com.example

import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer

import akka.actor.typed.receptionist.{ServiceKey, Receptionist}
import akka.actor.typed.{Behavior, SupervisorStrategy}
import akka.actor.typed.scaladsl.Behaviors
import akka.serialization.StringSerializer

import java.util.Properties
import com.example.model.ApiModel
import akka.serialization
import com.example.serde.JsonSerializer

import com.example.model.Message
import io.circe.Json

object KafkaDispatcher {
  sealed trait Command
  case class SendToKafka(topic: String, key: String, msg: Json) extends Command

  val serviceKey = ServiceKey[KafkaDispatcher.Command]("kafka-dispatcher")

  def apply(): Behavior[Command] = Behaviors
    .supervise[Command] {
      Behaviors.setup[Command] { ctx =>
        {
          val props = new Properties()
          props.put(
            ProducerConfig.CLIENT_ID_CONFIG,
            config.KafkaConfig.applicationID
          )
          props.put(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            config.KafkaConfig.bootstrapServers
          )
          props.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            classOf[StringSerializer]
          )
          props.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            classOf[JsonSerializer]
          )

          val kafkaProducer = new KafkaProducer[String, Json](props)

          ctx.system.receptionist ! Receptionist.Register(serviceKey, ctx.self)

          Behaviors.receiveMessage { case SendToKafka(topic, key, msg) =>
            kafkaProducer.send(new ProducerRecord(topic, key, msg))
            Behaviors.same
          }
        }
      }
    }
    .onFailure(SupervisorStrategy.resume)
}
