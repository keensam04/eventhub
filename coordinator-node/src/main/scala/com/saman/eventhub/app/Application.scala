package com.saman.eventhub.app

import java.net.InetAddress

import com.saman.eventhub.db.DBAccess
import com.saman.eventhub.db.mysql.MySQLAccess
import com.saman.eventhub.logic.CoordinatorNode
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
@ComponentScan
class Application extends EnvironmentAware {

  override def setEnvironment(environment: Environment) = {
    ApplicationProperties.environment = environment
  }

  def init(topics: List[String]): Unit = {
    val zkUtils = new ZkUtils(get("zk.server"))
    val db: DBAccess = new MySQLAccess(get("mysql.url"), get("mysql.user"), get("mysql.password"))

    sys.addShutdownHook({
      zkUtils.closeConnection
    })

    val coordinatorNode = new CoordinatorNode(zkUtils, db)
    coordinatorNode.start(topics)
  }
}

object Application extends App {
  SpringApplication.run(classOf[Application])

  val topics = args(0).split(",").toList
  new Application().init(topics)
}
