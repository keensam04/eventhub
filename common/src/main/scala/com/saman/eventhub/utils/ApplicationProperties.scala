package com.saman.eventhub.utils

import java.util.Properties

import org.springframework.core.env.Environment

object ApplicationProperties {

  var environment: Environment = _

  private lazy val serverAddress = {
    val host = "localhost"
    val port = environment.getProperty("local.server.port")
    s"$host:$port"
  }

  private lazy val properties = {
    val properties = new Properties()
    val inputStream = ApplicationProperties.getClass.getResourceAsStream("/application.properties")
    properties.load(inputStream)
    properties
  }

  def get(key: String) = {
    if ("server.address".equals(key))
      serverAddress
    else
      properties.getProperty(key)
  }
}
