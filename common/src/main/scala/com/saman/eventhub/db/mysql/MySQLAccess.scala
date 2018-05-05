package com.saman.eventhub.db.mysql

import java.sql.ResultSet

import com.saman.eventhub.db.DBAccess
import org.springframework.jdbc.core.{JdbcTemplate, ResultSetExtractor}
import org.springframework.jdbc.datasource.DriverManagerDataSource

class MySQLAccess(url: String, user: String, password: String) extends DBAccess {

  private lazy val jdbcTemplate = {
    val dataSource = new DriverManagerDataSource(url, user, password)
    dataSource.setDriverClassName("com.mysql.jdbc.Driver")
    new JdbcTemplate(dataSource)
  }

  override def insert(table: String, values: List[(String, Any)]): Unit = {
    if (values.isEmpty)
      return

    val fields = new StringBuilder("(")
    var args = new StringBuilder("(")
    var delimiter = ""
    for (value <- values) {
      fields.append(delimiter)
      args.append(delimiter)

      fields.append(value._1)
      args.append("?")

      delimiter = ", "
    }
    fields.append(")")
    args.append(")")
    val query = s"insert into $table ${fields.toString()} values ${args.toString()}"
    jdbcTemplate.update(query, values.map(value => value._2.asInstanceOf[Object]).toArray : _*)
  }

  override def read[T](query: String, resultSetExtractor: (ResultSet => T)): T = {
    jdbcTemplate.query(query, new ResultSetExtractor[T] {
      override def extractData(resultSet: ResultSet): T = resultSetExtractor(resultSet)
    })
  }
}
