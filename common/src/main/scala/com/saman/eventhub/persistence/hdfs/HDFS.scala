package com.saman.eventhub.persistence.hdfs

import java.io.StringWriter

import com.google.gson.{Gson, JsonObject}
import com.saman.eventhub.hdfs.Persistence
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._

object HDFS extends Persistence[Seq[JsonObject]] {

  private val uri = "hdfs://localhost:9000"

  private val gson = new Gson()

  override def saveData(path: String, data: Seq[JsonObject]): Unit = {
    val filePath = new Path(path)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val os = fs.create(filePath)
    os.writeBytes(gson.toJson(data.asJava))
    fs.close()
  }

  override def getData(path: String): Seq[JsonObject] = {
    val filePath = new Path(path)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val in = fs.open(filePath)
    val writer = new StringWriter
    IOUtils.copy(in, writer, "UTF-8")
    val raw = writer.toString
    gson.fromJson(raw, classOf[java.util.List[JsonObject]]).asScala.toList
  }
}

object TestFile extends App {
  val jsonData1 = new JsonObject()
  jsonData1.addProperty("num", 10)
  HDFS.saveData("hdfs/test", Seq(jsonData1))
}
