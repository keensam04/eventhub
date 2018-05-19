package com.saman.eventhub.kafka

import java.time.Instant
import java.util.Properties

import com.google.gson.{Gson, JsonObject}
import com.saman.eventhub.utils.ApplicationProperties.get
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.util.Random

object DataGenerator {

  private val random = new Random()
  private val gson = new Gson()
  private val kafkaProducer = {
    val properties = new Properties
    properties.put("bootstrap.servers", get("kafka.brokers"))
    properties.put("key.serializer", get("kafka.key.serializer"))
    properties.put("value.serializer", get("kafka.value.serializer"))
    new KafkaProducer[String, Array[Byte]](properties)
  }

  println(s"============> ${this.getClass.getName} started on ${get("server.address")}")

  /**
    * {
    *   "timestamp": 123,
    *   "publisher": "",
    *   "advertiser": "",
    *   "gender": "",
    *   "country": "",
    *   "click": 1,
    *   "price": 23.54
    * }
    */
  def start(interval: Long, topics: List[String]): Unit = {
    while (true) {
      Thread.sleep(interval)
      for (topic <- topics) {
        val time = System.currentTimeMillis()
        val message = gson.toJson(getPayload(time))
        val producerRecord = new ProducerRecord[String, Array[Byte]](topic, String.valueOf(time), message.getBytes())
        kafkaProducer.send(producerRecord, new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (exception != null)
              exception.printStackTrace()
            else
              println(s"${Instant.ofEpochMilli(time)}  $metadata  $message")
          }
        })
      }
    }
  }

  private def getPayload(time: Long): JsonObject = {
    val payload = new JsonObject
    payload.addProperty("timestamp", time)
    payload.addProperty("publisher", "PUBLISHER_".concat(random.nextInt(5).toString))
    payload.addProperty("advertiser", "ADVERTISER_".concat(random.nextInt(10).toString))
    payload.addProperty("gender", if (random.nextBoolean()) "MALE" else "FEMALE")
    payload.addProperty("country", "COUNTRY_".concat(random.nextInt(2).toString))
    payload.addProperty("click", random.nextInt(100))
    payload.addProperty("price", random.nextFloat() * 100)
    payload
  }
}
