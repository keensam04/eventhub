package com.saman.eventhub

import com.saman.eventhub.kafka.DataGenerator
import com.saman.eventhub.utils.ApplicationProperties
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

  def init(intervalBetweenMessages: Long, topics: List[String]): Unit = {
    DataGenerator.start(intervalBetweenMessages, topics)
  }
}

object Application extends App {
  SpringApplication.run(classOf[Application])

  val intervalBetweenMessages = args(0).toLong
  val topics = args(1).split(",").toList
  new Application().init(intervalBetweenMessages, topics)
}