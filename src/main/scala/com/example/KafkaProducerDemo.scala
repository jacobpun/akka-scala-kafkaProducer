package com.example

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.receptionist.{ServiceKey, Receptionist}

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{
  IntegerSerializer,
  StringSerializer
}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.io.Source
import java.util.Properties
import com.example.FileProcessorManager.ProcessFiles
import akka.actor.typed.SupervisorStrategy

object KafkaDispatcher {
  sealed trait Command
  case class SendToKafka(topic: String, msg: String) extends Command

  val serviceKey = ServiceKey[KafkaDispatcher.Command]("kafka-dispatcher")

  def apply(): Behavior[Command] = Behaviors
    .supervise[Command] {
      Behaviors.setup[Command] { ctx =>
        {
          val props = new Properties()
          props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConfig.applicationID)
          props.put(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            KafkaConfig.bootstrapServers
          )
          props.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            classOf[IntegerSerializer]
          )
          props.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            classOf[StringSerializer]
          )

          val kafkaProducer = new KafkaProducer[Int, String](props)

          ctx.system.receptionist ! Receptionist.Register(serviceKey, ctx.self)

          Behaviors.receiveMessage { case SendToKafka(topic, msg) =>
            kafkaProducer.send(new ProducerRecord(topic, msg))
            Behaviors.same
          }
        }
      }
    }
    .onFailure(SupervisorStrategy.resume)
}

object FileProcessor {
  sealed trait Command
  case class KafkaDispatcherListing(listings: Receptionist.Listing)
      extends Command
  case class ProcessFile(fileName: String) extends Command
  def apply(): Behavior[Command] = Behaviors.withStash(10) { buffer =>
    {
      Behaviors.setup { ctx =>
        {
          val adapter = ctx.messageAdapter(KafkaDispatcherListing)
          ctx.system.receptionist ! Receptionist.Subscribe(
            KafkaDispatcher.serviceKey,
            adapter
          )
          Behaviors.receiveMessage {
            case KafkaDispatcherListing(
                  KafkaDispatcher.serviceKey.Listing(listings)
                ) if (listings.size > 0) =>
              buffer.unstashAll(processFile(listings.head))
            case KafkaDispatcherListing(_) => Behaviors.same
            case other =>
              buffer.stash(other)
              Behaviors.same
          }
        }
      }
    }
  }

  def processFile(
      KafkaDispatcherRef: ActorRef[KafkaDispatcher.Command]
  ): Behavior[Command] = Behaviors.receive { (ctx, msg) =>
    {
      msg match {
        case ProcessFile(fileName) =>
          ctx.log.info(s"Processing file $fileName")
          for (line <- Source.fromFile(fileName).getLines()) {
            KafkaDispatcherRef ! KafkaDispatcher.SendToKafka(
              KafkaConfig.topicName,
              line
            )
          }
          Behaviors.stopped
        case _ => Behaviors.same
      }
    }
  }
}

object FileProcessorManager {
  sealed trait Command
  case class ProcessFiles(files: List[String]) extends Command

  def apply(): Behavior[Command] = Behaviors.receive { (ctx, msg) =>
    {
      msg match {
        case ProcessFiles(files) =>
          ctx.log.info(s"Receiced process files command for files: $files")
            files.foreach(file => {
              ctx.log.info("Processing file: {}", file)
              val processor = ctx.spawn(FileProcessor(), s"fileprocessor$file")
              processor ! FileProcessor.ProcessFile(file)
            })
          Behaviors.same
      }
    }
  }
}

object Guardian {
  def apply(): Behavior[FileProcessorManager.Command] = {
    Behaviors.setup[FileProcessorManager.Command] { ctx =>
      {
        val manager = ctx.spawn(FileProcessorManager(), "manager")
        ctx.spawnAnonymous(KafkaDispatcher())
        Behaviors.receiveMessage[FileProcessorManager.Command] {
          case ProcessFiles(files) =>
            manager ! ProcessFiles(files)
            Behaviors.same
        }
      }
    }
  }
}

object FileProcessorApp extends App {
  val actorSystem = ActorSystem(Guardian(), "fileProcessorApp")
  actorSystem ! FileProcessorManager.ProcessFiles(
    List("file1.txt", "file2.txt")
  )
}

object KafkaConfig {
  val applicationID = "ExampleProducer";
  val bootstrapServers = "localhost:9092,localhost:9093";
  val topicName = "file-content-topic";
}
