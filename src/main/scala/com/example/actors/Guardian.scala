package com.example

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object Guardian {
  import FileProcessorManager._
  def apply(): Behavior[Command] = {
    Behaviors.setup[Command] { ctx =>
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
