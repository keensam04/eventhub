package com.saman.eventhub.persistence

import java.util

import com.google.common.collect.{ListMultimap, MultimapBuilder}
import com.google.gson.JsonObject
import com.saman.eventhub.persistence.hdfs.HDFS

import scala.collection.JavaConversions._

class InMemoryData(persistence: HDFS) {

  def save(segment: String, data: JsonObject): Unit = {
    val values = InMemoryData._data.get(segment)
    values.add(data)
  }

  def save(segment: String, allData: Seq[JsonObject]): Unit = {
    InMemoryData._data.putAll(segment, allData)
  }

  def persist(segment: String): Unit = {
    val values = InMemoryData._data.get(segment)
    persistence.saveData(segment, values)
  }

  def delete(segment: String) = persistence.delete(segment)

  def clear(segment: String) = InMemoryData._data.removeAll(segment)

  def clearAll() = InMemoryData._data.clear()

  def get(segments: Seq[String]): Seq[JsonObject] = {
    InMemoryData.get(segments)
  }
}

object InMemoryData {

  private val _data: ListMultimap[String, JsonObject] =
    MultimapBuilder.treeKeys().arrayListValues().build[String, JsonObject]()

  def get(segments: Seq[String]): Seq[JsonObject] = {
    val allData = new util.ArrayList[JsonObject]()
    for (segment <- segments) {
      allData.addAll(InMemoryData._data.get(segment))
    }
    allData
  }
}
