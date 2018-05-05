package com.saman.eventhub.db

import java.sql.ResultSet

trait DBAccess {

  def insert(table: String, values: List[(String, Any)]): Unit

  def read[T](query: String, resultSetExtractor: (ResultSet => T)): T

}
