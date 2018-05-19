package com.saman.eventhub.hdfs

trait Persistence[T] {

  def saveData(path: String, data: T): Boolean

  def delete(path: String): Unit

  def getData(path: String): Option[T]

}
