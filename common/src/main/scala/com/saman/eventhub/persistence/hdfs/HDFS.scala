package com.saman.eventhub.persistence.hdfs

import java.io.StringWriter

import com.google.gson.{Gson, JsonObject}
import com.saman.eventhub.hdfs.Persistence
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

import scala.collection.JavaConverters._

class HDFS(uri: String) extends Persistence[Seq[JsonObject]] {

  private val gson = new Gson()

  override def saveData(path: String, data: Seq[JsonObject]): Boolean = {
    var fs: FileSystem = null
    var os: FSDataOutputStream = null
    try {
      val filePath = new Path(uri + path)
      val conf = new Configuration()
      fs = filePath.getFileSystem(conf)
      os = fs.create(filePath)
      os.writeBytes(gson.toJson(data.asJava))
      true
    } catch {
      case ex: Exception => false
    } finally {
      if (os != null)
        os.close()
      if (fs != null)
        fs.close()
    }
  }

  override def getData(path: String): Option[Seq[JsonObject]] = {
    var fs: FileSystem = null
    var in: FSDataInputStream = null
    try {
      val filePath = new Path(uri + path)
      val conf = new Configuration()
      fs = FileSystem.get(conf)
      in = fs.open(filePath)
      val writer = new StringWriter
      IOUtils.copy(in, writer, "UTF-8")
      val raw = writer.toString
      Some(gson.fromJson(raw, classOf[java.util.List[JsonObject]]).asScala.toList)
    } catch {
      case ex: Exception => None
    } finally {
      if (in != null)
        in.close()
      if (fs != null)
        fs.close()
    }
  }
}
