package com.saman.eventhub.zk

import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener, PathChildrenCache, PathChildrenCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode

class ZkUtils(serverString: String) {

  private lazy val client: CuratorFramework = {
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val client = CuratorFrameworkFactory.newClient(serverString, retryPolicy)
    client.start()
    client
  }

  /**
    *
    * @param path
    * @return zPath created
    */
  def createEphemeralZNode(path: String): String = {
    createIfNotExists(path.substring(0, path.lastIndexOf("/")), () => null)
    client.create().withMode(CreateMode.EPHEMERAL).forPath(path, null)
  }

  /**
    *
    * @param path
    * @param dataSupplier
    * @return None if node exists or zPath name if successfully created
    */
  def createIfNotExists(path: String, dataSupplier: () => Array[Byte]): Option[String] = {
    if (client.checkExists().forPath(path) != null)
      None
    else {
      val paths = if (path.startsWith("/")) path.split("/") else s"/$path".split("/")
      var _path = ""
      for (i <- 1 to (paths.length - 2)) {
        _path = s"${_path}/${paths(i)}"
        if (client.checkExists().forPath(_path) == null)
          client.create().forPath(_path)
      }
      Some(client.create().forPath(path, dataSupplier()))
    }
  }

  def setData(path: String, dataSupplier: () => Array[Byte]) = {
    client.create().orSetData().forPath(path, dataSupplier())
  }

  def deletePath(path: String): Boolean = {
    if (client.checkExists().forPath(path) == null)
      false
    else {
      client.delete().guaranteed().forPath(path)
      true
    }
  }

  def read[T](path: String, dataReader: Array[Byte] => T): Option[T] = {
    val data = client.getData.forPath(path)
    if (data == null)
      None
    else
      Some(dataReader(data))
  }

  def watch(path: String, watcher: PathChildrenCacheListener): Unit = {
    val allChildren = new PathChildrenCache(client, path, true)
    allChildren.getListenable.addListener(watcher)
    allChildren.start()
  }

  def watch(path: String, dataConsumer: Array[Byte] => Unit): Unit = {
    val node = new NodeCache(client, path)
    node.getListenable.addListener(new NodeCacheListener {
      val previousData: Array[Byte] = null
      override def nodeChanged(): Unit = {
        dataConsumer(node.getCurrentData.getData)
      }
    })
    node.start()
  }

  def closeConnection = client.close()
}
