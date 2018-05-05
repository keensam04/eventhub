package com.saman.eventhub.hdfs

import com.google.gson.JsonArray

// https://stackoverflow.com/questions/32380272/how-to-write-to-hdfs-using-scala
trait Persistence[T] {

  def saveData(path: String, data: T): Unit

  def getData(path: String): T

}
