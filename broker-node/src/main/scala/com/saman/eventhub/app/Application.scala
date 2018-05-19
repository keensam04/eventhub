package com.saman.eventhub.app

import com.saman.eventhub.utils.ApplicationProperties
import com.saman.eventhub.utils.ApplicationProperties.get
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
    ApplicationProperties.environment = environment
  }

  def init: Unit = {
    println(s"============> ${this.getClass.getName} started on ${get("server.address")}")
  }
}

object Application extends App {
  SpringApplication.run(classOf[Application])
  new Application().init
}
