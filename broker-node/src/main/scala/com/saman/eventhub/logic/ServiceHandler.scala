package com.saman.eventhub.logic

import java.lang.{Long => jLong}
import java.time.Instant

import com.google.common.collect.Range
import com.google.gson.{Gson, JsonElement}
import com.saman.eventhub.rest.Aggregations
import com.saman.eventhub.utils.ApplicationProperties.get
import com.saman.eventhub.zk.ZkUtils
import org.springframework.web.client.RestTemplate

import scala.collection.mutable
import scala.util.Try

object ServiceHandler {

  val zkUtils = new ZkUtils(get("zk.server"))
  val restClient = new RestTemplate()
  val gson = new Gson()

  def getResource(source: String, timerange: String, publisher: String, advertiser: String,
                  gender: String, country: String, aggregation: String, metric: String): String = {
    val listOfResponse = getRanges(source, timerange)
      .map(segment => (segment._1, Try(zkUtils.read(segment._2, bytes => new String(bytes))) getOrElse None))
      .filter(segment => segment._2.isDefined)
      .map(segment => (segment._1, segment._2.get))
      .map(segment => {
        var request = new mutable.StringBuilder()
          .append("http://")
          .append(segment._2)
          .append("/resource")
          .append(s"?source=$source")
          .append(s"&timerange=${Instant.ofEpochMilli(segment._1.lowerEndpoint()).toString}-${Instant.ofEpochMilli(segment._1.upperEndpoint()).toString}")

        if (publisher != null)
          request = request.append(s"&publisher=$publisher")
        if (advertiser != null)
          request = request.append(s"&advertiser=$advertiser")
        if (gender != null)
          request = request.append(s"&gender=$gender")
        if (country != null)
          request = request.append(s"&country=$country")
        if (aggregation != null)
          request = request.append(s"&aggregation=$aggregation")
        if (metric != null)
          request = request.append(s"&metric=$metric")

        val response = restClient.getForEntity(request.toString(), classOf[String])
        if (response.getStatusCode.is2xxSuccessful())
          Some(gson.fromJson(response.getBody, classOf[JsonElement]))
        else None
      })
      .filter(response => response.isDefined)
      .map(response => response.get)
      .filter(json => !json.isJsonNull)
      .toList

    gson.toJson(Aggregations.operate(listOfResponse, metric, aggregation))
  }

  private def getRanges(source: String, timerange: String): Map[Range[jLong], String] = {
    val fromAndToTime = timerange.split("Z-")
    val from = Instant.parse(s"${fromAndToTime(0)}Z").toEpochMilli
    val to = Instant.parse(fromAndToTime(1)).toEpochMilli
    val segmentInterval = get("eventhub.segment.interval").toLong

    val subRanges = mutable.HashMap.empty[Range[jLong], String]
    var lowerBound = from - (from % segmentInterval)
    do {
      val upperBound = lowerBound + segmentInterval
      val segment = s"/segments/$source/$lowerBound-$upperBound"
      val rangeLower = if (lowerBound < from) from else lowerBound
      val rangeUpper = if (upperBound > to) to else upperBound
      subRanges.put(Range.openClosed(rangeLower, rangeUpper), segment)
      lowerBound = lowerBound + segmentInterval
    } while (lowerBound <= to)
    subRanges.toMap
  }
}
