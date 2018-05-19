package com.saman.eventhub.app

import com.saman.eventhub.db.mysql.MySQLAccess
import com.saman.eventhub.logic.RealTimeNode
import com.saman.eventhub.persistence.hdfs.HDFS
import com.saman.eventhub.utils.ApplicationProperties
import com.saman.eventhub.utils.ApplicationProperties.get
import com.saman.eventhub.zk.ZkUtils
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.autoconfigure.{EnableAutoConfiguration, SpringBootApplication}
import org.springframework.context.EnvironmentAware
import org.springframework.context.annotation.ComponentScan
import org.springframework.core.env.Environment

@SpringBootApplication
@EnableAutoConfiguration(exclude = Array(classOf[DataSourceAutoConfiguration]))
@ComponentScan(basePackages = Array[String]("com.saman.eventhub*"))
class Application extends EnvironmentAware {

  override def setEnvironment(environment: Environment) = {
    ApplicationProperties.environment = environment;
  }

  def init: Unit = {
    val zkUtils = new ZkUtils(get("zk.server"))
    val persistence = new HDFS(get("hdfs.uri"))
    val db = new MySQLAccess(get("mysql.url"), get("mysql.user"), get("mysql.password"))

    sys.addShutdownHook({
      zkUtils.closeConnection
    })

    val realTimeNode = new RealTimeNode(zkUtils, persistence, db)
    realTimeNode.start
  }

}

object Application extends App {
  SpringApplication.run(classOf[Application])
  new Application().init
}
