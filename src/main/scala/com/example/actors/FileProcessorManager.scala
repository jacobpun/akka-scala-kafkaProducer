package com.example

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

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