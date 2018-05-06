package com.saman.eventhub.persistence.hdfs

import com.google.gson.JsonObject

object HDFSTest extends App{

  private val uri = "file:///tmp/eventhub/"
  private val hdfs = new HDFS(uri)

  val jsonObject = new JsonObject()
  jsonObject.addProperty("key", "value")
  hdfs.saveData("hdfs/test.json", Seq(jsonObject))
  print(hdfs.getData("hdfs/test.json"))
}
