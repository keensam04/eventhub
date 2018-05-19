package com.saman.eventhub.logic

import java.lang.{Long => jLong}
import java.util.Properties

import com.google.common.collect.Range
import com.google.gson.{Gson, JsonObject}
import com.saman.eventhub.db.DBAccess
import com.saman.eventhub.persistence.InMemoryData
import com.saman.eventhub.persistence.hdfs.HDFS
import com.saman.eventhub.utils.ApplicationProperties.get
import com.saman.eventhub.zk.ZkUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class RealTimeNode(zkUtils: ZkUtils, persistence: HDFS, db: DBAccess) {

  private val gson = new Gson()
  private val node = s"/status/real-time-nodes/${get("server.address")}"
  private val allSources = new mutable.HashSet[String]()
  private val inMemoryData = new InMemoryData(persistence)
  private val kafkaConsumer = {
    val properties = new Properties
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, get("kafka.brokers"))
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "real-time-nodes")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, get("kafka.key.deserializer"))
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, get("kafka.value.deserializer"))
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    new KafkaConsumer[String, Array[Byte]](properties)
  }

  println(s"============> ${this.getClass.getName} started on ${get("server.address")}")

  def merge(segments: mutable.Set[String]) = {
    Future {
      segments.foreach(segment => {
        val segmentParts = segment.split("/")
        val source = segmentParts(0)
        val range = segmentParts(1).split("-")
        val _range = Range.openClosed[jLong](range(0).toLong, range(1).toLong)
        val __range = getSubsegments(_range)
          .map(sub => getSegmentPath(source, stringify(sub)))
        val allData = inMemoryData.get(__range)
        inMemoryData.save(segment, allData)
        inMemoryData.persist(segment)
        __range.foreach(segment => {
          inMemoryData.delete(segment)
          inMemoryData.clear(segment)
        })
      })
    }
  }

  def start: Unit = {

    zkUtils.createEphemeralZNode(node)

    zkUtils.watch("/tasks/real-time-nodes/new-tasks", bytes => {
      if (bytes != null) {
        val string = new String(bytes)
        val sources = string.split(",")
        for (source <- sources) {
          val _source = source.trim
          if (!"".equals(_source))
            allSources.add(_source)
        }
      }
    })

    while (true) {
      val segmentRange = getSegmentRange()
      val segments = allSources
        .map(source => {
          val segmentRangeAsString = stringify(segmentRange)
          val segmentPath = getSegmentPath(source, segmentRangeAsString)
          val response = zkUtils.createIfNotExists(s"/segments/$segmentPath", () => get("server.address").getBytes)
          response
        })
        .filter(segment => segment.isDefined)
        .map(segment => {
          val _segment = segment.get
          _segment.replace("/segments/", "")
        })

      if (!segments.isEmpty) {
        zkUtils.setData(node, () => segments.mkString(",").getBytes)

        val subRanges = getSubsegments(segmentRange)

        val topics = segments.map(segment => segment.split("/")(0)).toSet
        kafkaConsumer.subscribe(topics.asJava)
        var untilOffsets = mutable.Map.empty[TopicPartition, OffsetAndMetadata]
        for (subRange <- subRanges) {
          untilOffsets = consumeFromKafka(subRange, untilOffsets)
        }
        merge(segments).onComplete {
          case Success(value) => topics.foreach(topic => {
            db.insert("segments",
              List(("source", topic),
                ("epoch_from", segmentRange.lowerEndpoint()),
                ("epoch_to", segmentRange.upperEndpoint())))
            segments.foreach(segment => {
              zkUtils.deletePath(s"/segments/$segment")
              inMemoryData.clear(segment)
            })

          })
          case Failure(e) => e.printStackTrace
        }
        kafkaConsumer.unsubscribe()
      }
    }
  }

  private def consumeFromKafka(range: Range[jLong],
                               startOffset: mutable.Map[TopicPartition, OffsetAndMetadata]): mutable.Map[TopicPartition, OffsetAndMetadata] = {
    startOffset.foreach(offsetInfo => kafkaConsumer.seek(offsetInfo._1, offsetInfo._2.offset()))
    val offsetsToCommit = mutable.HashMap.empty[TopicPartition, OffsetAndMetadata]
    while (true) {
      val records = kafkaConsumer.poll(500)
      val recordIterator = records.iterator()
      while (recordIterator.hasNext) {
        val record = recordIterator.next()
        val key = record.key().toLong
        val segment = getSegmentPath(record.topic(), stringify(range))
        if (range.contains(key)) {
          saveToInMemory(segment, record.value())
          offsetsToCommit += (new TopicPartition(record.topic(), record.partition()) -> new OffsetAndMetadata(record.offset(), ""))
        } else if (key >= range.upperEndpoint()) {
          inMemoryData.persist(segment)
          kafkaConsumer.commitSync(offsetsToCommit.asJava)
          return offsetsToCommit
        }
      }
    }
    mutable.HashMap.empty
  }

  private def saveToInMemory(segment: String, value: Array[Byte]) = {
    val string = new String(value)
    val jsonObject = gson.fromJson(string, classOf[JsonObject])
    inMemoryData.save(segment, jsonObject)
  }

  private def getSegmentRange() = {
    val currentTime = System.currentTimeMillis()
    val segmentInterval = get("eventhub.segment.interval").toLong
    val start = (currentTime - (currentTime % segmentInterval))
    val end = start + segmentInterval
    Range.closedOpen[jLong](start, end)
  }

  private def getSubsegments(range: Range[jLong]): List[Range[jLong]] = {
    val segmentInterval = get("eventhub.subsegment.interval").toLong
    val subRanges = mutable.ListBuffer.empty[Range[java.lang.Long]]
    val upperMax = range.upperEndpoint()
    var lower = range.lowerEndpoint()
    do {
      subRanges += (Range.openClosed(lower, lower + segmentInterval))
      lower = lower + segmentInterval
    } while (lower < upperMax)
    subRanges.toList
  }

  private def stringify(range: Range[jLong]) = {
    range.lowerEndpoint().toString.concat("-").concat(range.upperEndpoint().toString)
  }

  private def getSegmentPath(source: String, range: String) = s"$source/$range"

}
