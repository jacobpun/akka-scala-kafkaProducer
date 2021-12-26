package com.example.serde

import org.apache.kafka.common.serialization.Serializer
import com.example.model.ApiModel
import io.circe.Json


case class JsonSerializer() extends Serializer[Json]  {
  override def serialize(topic: String, data: Json): Array[Byte] = {
    data.toString().getBytes()
  }
}
