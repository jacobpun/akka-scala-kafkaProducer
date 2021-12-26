package com.example

import akka.actor.typed.ActorSystem

import Guardian._

object FileProcessorApp extends App {
  val actorSystem = ActorSystem(Guardian(), "fileProcessorApp")
  actorSystem ! FileProcessorManager.ProcessFiles(
    List("file1.txt", "file2.txt")
  )
}