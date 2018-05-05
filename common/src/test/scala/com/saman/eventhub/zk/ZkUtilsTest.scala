package com.saman.eventhub.zk

import java.util.concurrent.CountDownLatch

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener}

object ZkUtilsTest extends App {

  val _wait = new CountDownLatch(1)

  val zkUtils1 = new ZkUtils("localhost:2181")
  val zkUtils2 = new ZkUtils("localhost:2181")

  val _1 = zkUtils1.createIfNotExists("/testPath", () => "testData".getBytes)
  val _2 = zkUtils1.createIfNotExists("/testPath", () => "testData".getBytes)
  val _3 = zkUtils1.read("/testPath", data => new String(data))
  val _4 = zkUtils1.setData("/testPath", "testData2".getBytes)
  val _5 = zkUtils1.read("/testPath", data => new String(data))
  val _6 = zkUtils1.deletePath("/testPath")
  val _7 = zkUtils1.createIfNotExists("/testPath", () => "testData3".getBytes)
  val _8 = zkUtils1.read("/testPath", data => new String(data))
  zkUtils2.watch("/ephemeralPath", new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      val data = event.getData
      val initialData = event.getInitialData
      val eventType = event.getType
    }
  })
  zkUtils1.createEphemeralZNode("/ephemeralPath/path4", () => "testData4".getBytes())
  zkUtils1.closeConnection

  _wait.await()
}
