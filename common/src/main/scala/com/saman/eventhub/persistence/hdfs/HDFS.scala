package com.saman.eventhub.persistence.hdfs

import java.io.StringWriter
import java.util.{Collection => jCollection}

import com.google.gson.reflect.TypeToken
import com.google.gson.{Gson, JsonObject}
import com.saman.eventhub.hdfs.Persistence
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

class HDFS(uri: String) extends Persistence[jCollection[JsonObject]] {

  private val gson = new Gson()
  private val conf = new Configuration()

  override def saveData(path: String, data: jCollection[JsonObject]): Boolean = {
    var fs: FileSystem = null
    var os: FSDataOutputStream = null
    try {
      val filePath = new Path(uri + path)
      fs = filePath.getFileSystem(conf)
      os = fs.create(filePath)
      os.writeBytes(gson.toJson(data))
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

  override def delete(path: String): Unit = {
    var fs: FileSystem = null
    try {
      val filePath = new Path(uri + path)
      fs = filePath.getFileSystem(conf)
      fs.delete(filePath, false)
    } finally {
      if (fs != null)
        fs.close()
    }
  }

  override def getData(path: String): Option[jCollection[JsonObject]] = {
    var fs: FileSystem = null
    var in: FSDataInputStream = null
    try {
      val filePath = new Path(uri + path)
      fs = FileSystem.get(conf)
      in = fs.open(filePath)
      val writer = new StringWriter
      IOUtils.copy(in, writer, "UTF-8")
      val raw = writer.toString
      val _type = new TypeToken[java.util.List[JsonObject]](){}.getType
      Some(gson.fromJson(raw, _type))
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
