package com.saman.eventhub.hdfs

// https://stackoverflow.com/questions/32380272/how-to-write-to-hdfs-using-scala
trait Persistence[T] {

  def saveData(data: Seq[T]): Unit

  def getData(path: String): Seq[T]

}
