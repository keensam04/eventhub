package com.saman.eventhub.zk

import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheListener}
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
    * @param dataSupplier
    * @return zPath created
    */
  def createEphemeralZNode(path: String, dataSupplier: () => Array[Byte]): String = {
    client.create().withMode(CreateMode.EPHEMERAL).forPath(path, dataSupplier())
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
    else
      Some(client.create().forPath(path, dataSupplier()))
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

  def closeConnection = client.close()
}
