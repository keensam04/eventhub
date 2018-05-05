package com.saman.eventhub.db.h2

import java.sql.ResultSet

import com.saman.eventhub.db.DBAccess

class H2Utils extends DBAccess {
  override def insert(table: String, values: List[(String, Any)]): Unit = ???

  override def read[T](query: String, resultSetExtractor: ResultSet => T): T = ???
}
