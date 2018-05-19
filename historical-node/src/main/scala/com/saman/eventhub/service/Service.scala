package com.saman.eventhub.service

import java.util.{Map => jMap}

import com.saman.eventhub.logic.ServiceHandler
import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RequestParam, RestController}

@RestController
@RequestMapping(value = Array[String]("/"))
class Service {

  @RequestMapping(value = Array[String]("whoareyou"),
    method = Array[RequestMethod](RequestMethod.GET),
    produces = Array[String]("text/plain"))
  def ping: String = {
    "historical-node"
  }

  @RequestMapping(value = Array[String]("resource"),
    method = Array[RequestMethod](RequestMethod.GET),
    produces = Array[String]("application/json"))
  def getResource(@RequestParam(value = "source", required = true) source: String,
                  @RequestParam(value = "timerange", required = true) timerange: String,
                  @RequestParam(value = "publisher", required = false) publisher: String,
                  @RequestParam(value = "advertiser", required = false) advertiser: String,
                  @RequestParam(value = "gender", required = false) gender: String,
                  @RequestParam(value = "country", required = false) country: String,
                  @RequestParam(value = "aggregation", required = false) aggregation: String,
                  @RequestParam(value = "metric", required = false) metric: String): String = {
    ServiceHandler.getResource(source, timerange, publisher, advertiser, gender, country, aggregation, metric)
  }

}
