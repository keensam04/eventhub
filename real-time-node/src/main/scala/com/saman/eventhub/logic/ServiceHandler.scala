package com.saman.eventhub.logic

import java.lang.{Long => jLong}
import java.time.Instant

import com.google.common.collect.Range
import com.google.gson.Gson
import com.saman.eventhub.persistence.InMemoryData
import com.saman.eventhub.rest.Aggregations
import com.saman.eventhub.utils.ApplicationProperties.get
import com.saman.eventhub.zk.ZkUtils

import scala.collection.mutable

object ServiceHandler {

  val zkUtils = new ZkUtils(get("zk.server"))
  val gson = new Gson()

  def getResource(source: String, timerange: String, publisher: String, advertiser: String,
                  gender: String, country: String, aggregation: String, metric: String): String = {


    val timeRange = getRange(timerange)
    val ranges = getRanges(source, timeRange)
    val _ranges = ranges.map(range => range._2).toSeq
    val result = InMemoryData.get(_ranges)
      .filter(each => {
        if (each.get("timestamp").getAsLong < timeRange._1 ||
          each.get("timestamp").getAsLong > timeRange._2)
          false
        else
            true
      })
      .filter(each => {
        if (publisher != null)
          publisher.equals(each.get("publisher").getAsString)
        else
          true
      })
      .filter(each => {
        if (advertiser != null)
          advertiser.equals(each.get("advertiser").getAsString)
        else
          true
      })
      .filter(each => {
        if (gender != null)
          gender.equals(each.get("gender").getAsString)
        else
          true
      })
      .filter(each => {
        if (country != null)
          country.equals(each.get("country").getAsString)
        else
          true
      })

    gson.toJson(Aggregations.operate(result.toList, metric, aggregation))
  }

  private def getRange(timerange: String): (Long, Long) = {
    val fromAndToTime = timerange.split("Z-")
    val from = Instant.parse(s"${fromAndToTime(0)}Z").toEpochMilli
    val to = Instant.parse(fromAndToTime(1)).toEpochMilli
    (from, to)
  }

  private def getRanges(source: String, timerange: (Long, Long)): Map[Range[jLong], String] = {
    val from = timerange._1
    val to = timerange._2
    val segmentInterval = get("eventhub.segment.interval").toLong

    val subRanges = mutable.HashMap.empty[Range[jLong], String]
    var lowerBound = from - (from % segmentInterval)
    do {
      val upperBound = lowerBound + segmentInterval
      val segment = s"/segments/$source/$lowerBound-$upperBound"
      val rangeLower = if (lowerBound < from) from else lowerBound
      val rangeUpper = if (upperBound > to) to else upperBound
      val _range = Range.openClosed[jLong](rangeLower, rangeUpper)
      subRanges.put(_range, segment)
      for (each <- getSubsegments(source, _range)) {
        subRanges.put(each._1, each._2)
      }
      lowerBound = lowerBound + segmentInterval
    } while (lowerBound <= to)
    subRanges.toMap
  }

  private def getSubsegments(source: String, range: Range[jLong]): Map[Range[jLong], String] = {
    val from: Long = range.lowerEndpoint()
    val to: Long = range.upperEndpoint()
    val segmentInterval = get("eventhub.subsegment.interval").toLong

    val subRanges = mutable.HashMap.empty[Range[jLong], String]
    var lowerBound = from - (from % segmentInterval)
    do {
      val upperBound = lowerBound + segmentInterval
      val segment = s"$source/$lowerBound-$upperBound"
      val rangeLower = if (lowerBound < from) from else lowerBound
      val rangeUpper = if (upperBound > to) to else upperBound
      subRanges.put(Range.openClosed(rangeLower, rangeUpper), segment)
      lowerBound = lowerBound + segmentInterval
    } while (lowerBound <= to)
    subRanges.toMap
  }


}
