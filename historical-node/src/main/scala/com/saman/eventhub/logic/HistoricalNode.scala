package com.saman.eventhub.logic

import java.util

import com.google.gson.{Gson, JsonObject}
import com.saman.eventhub.db.DBAccess
import com.saman.eventhub.persistence.InMemoryData
import com.saman.eventhub.persistence.hdfs.HDFS
import com.saman.eventhub.utils.ApplicationProperties.get
import com.saman.eventhub.zk.ZkUtils

import scala.collection.JavaConversions._
import scala.collection.mutable

class HistoricalNode(zkUtils: ZkUtils, persistence: HDFS, db: DBAccess) {

  private val gson = new Gson()
  private val node = s"/status/historical-nodes/${get("server.address")}"
  private val inMemoryData = new InMemoryData(persistence)

  println(s"============> ${this.getClass.getName} started on ${get("server.address")}")

  def start: Unit = {

    zkUtils.createEphemeralZNode(node)

    val allSources = new mutable.HashSet[String]()
    zkUtils.watch("/tasks/historical-nodes/new-tasks", bytes => {
      if (bytes != null) {
        val string = new String(bytes)
        val sources = string.split(",")
        for (source <- sources) {
          val _source = source.trim
          if (!"".equals(_source))
            allSources.add(_source)
        }
      }
    })

    while (true) {

      allSources
        .filter(segment => {
          val response = zkUtils.createIfNotExists(s"/segments/$segment", () => get("server.address").getBytes)
          response.isDefined
        })
        .foreach(segment => {
          zkUtils.setData(node, () => segment.getBytes())
          val data = persistence.getData(segment)
          data match {
            case Some(_data) => inMemoryData.save(segment, new util.ArrayList[JsonObject](_data))
            case None => {}
          }
          zkUtils.setData(node, () => null)
        })
    }
  }
}
