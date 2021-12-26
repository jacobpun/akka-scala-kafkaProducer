package com.example.model

import io.circe.Encoder
import io.circe.generic.semiauto._  
import io.circe.Json
import io.circe.syntax._

sealed trait ApiModel {
  val id: String
}

case class Message(
    id: String,
    message: String,
    source: String
) extends ApiModel

object ApiModel {
  implicit val messageEncoder: Encoder[Message] = deriveEncoder[Message] 
  implicit def messageToJson(message: Message): Json = message.asJson
}
