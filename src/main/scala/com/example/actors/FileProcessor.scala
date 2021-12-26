package com.example

import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.{Behavior, ActorRef}
import akka.actor.typed.scaladsl.Behaviors
import scala.io.Source
import com.example.model.Message
import scala.util.Try

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
            val idAndMessage = line.split(",", 2)
            val messageTry =
              Try(
                Message(
                  idAndMessage(0).trim(),
                  idAndMessage(1).trim(),
                  fileName
                )
              )
            messageTry.map(msg => {
              KafkaDispatcherRef ! KafkaDispatcher.SendToKafka(
                config.KafkaConfig.topicName,
                msg.id,
                msg
              )
            })
          }
          Behaviors.stopped
        case _ => Behaviors.same
      }
    }
  }
}
