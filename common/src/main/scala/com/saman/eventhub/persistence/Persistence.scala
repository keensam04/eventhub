package com.saman.eventhub.hdfs

trait Persistence[T] {

  def saveData(path: String, data: T): Boolean

  def getData(path: String): Option[T]

}
