package com.saman.eventhub.rest

import com.google.gson.{Gson, JsonArray, JsonElement, JsonObject}

import scala.util.Try

object Aggregations {

  private val gson = new Gson()

  def max(items: List[JsonObject], key: String): JsonObject = {
    items.reduce((item1,item2) => Try({
      val value1 = item1.get(key).getAsInt
      val value2 = item2.get(key).getAsInt
      if (value1 > value2) item1 else item2
    }) getOrElse ({
      val value1 = item1.get(key).getAsFloat
      val value2 = item2.get(key).getAsFloat
      if (value1 > value2) item1 else item2
    }))
  }

  def min(items: List[JsonObject], key: String): JsonObject = {
    items.reduce((item1,item2) => Try({
      val value1 = item1.get(key).getAsInt
      val value2 = item2.get(key).getAsInt
      if (value1 > value2) item2 else item1
    }) getOrElse ({
      val value1 = item1.get(key).getAsFloat
      val value2 = item2.get(key).getAsFloat
      if (value1 > value2) item2 else item1
    }))
  }

  def sumOfInt(items: List[JsonObject], key: String): Int = {
    items
      .map(item => item.get(key).getAsInt).sum
  }

  def sumOfFloat(items: List[JsonObject], key: String): Float = {
    items
      .map(item => item.get(key).getAsFloat).sum
  }

  def average(items: List[JsonObject], key: String): Double = {
    items
      .map(item => item.get(key).getAsString.toDouble)
      .reduce((value1,value2) => (value1 + value2) / 2)
  }

  def operate(items: List[JsonElement], key: String, operation: String): JsonElement = {
    operation match {
      case "MIN" => min(items.map(item => item.asInstanceOf[JsonObject]), key)
      case "MAX" => max(items.map(item => item.asInstanceOf[JsonObject]), key)
      case "SUM_INT" =>
        val jsonObject = new JsonObject()
        jsonObject.addProperty(key, sumOfInt(items.map(item => item.asInstanceOf[JsonObject]), key))
        jsonObject
      case "SUM_FLOAT" =>
        val jsonObject = new JsonObject()
        jsonObject.addProperty(key, sumOfFloat(items.map(item => item.asInstanceOf[JsonObject]), key))
        jsonObject
      case "AVERGAGE" =>
        val jsonObject = new JsonObject()
        jsonObject.addProperty(key, average(items.map(item => item.asInstanceOf[JsonObject]), key))
        jsonObject
      case _ =>
        gson.fromJson(items.mkString("[", ",", "]"), classOf[JsonArray])
    }
  }
}
