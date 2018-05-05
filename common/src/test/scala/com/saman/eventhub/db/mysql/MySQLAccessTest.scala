package com.saman.eventhub.db.mysql

import scala.collection.mutable.ListBuffer

object MySQLAccessTest extends App {

  val mySQLAccess = new MySQLAccess("jdbc:mysql://localhost:3306/eventhub", "root", "root")

  mySQLAccess.insert("segments", List(("source", "app"),("epoch_from", 12345),("epoch_to", 23456)))

  val allSegments = mySQLAccess.read("select source, epoch_from, epoch_to, last_updated from segments", rs => {
    var listBuffer = new ListBuffer[(String, Long, Long, Long)]()
    while (rs.next()) {
      val source = rs.getString("source")
      val epoch_from = rs.getLong("epoch_from")
      val epoch_to = rs.getLong("epoch_to")
      val last_updated = rs.getTimestamp("last_updated").getTime
      listBuffer += ((source, epoch_from, epoch_to, last_updated))
    }
    listBuffer.toList
  })

  println(allSegments)

}
