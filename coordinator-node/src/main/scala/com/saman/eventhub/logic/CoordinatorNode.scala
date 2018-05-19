package com.saman.eventhub.logic

import java.util.concurrent.{Executors, TimeUnit}

import com.saman.eventhub.db.DBAccess
import com.saman.eventhub.utils.ApplicationProperties.get
import com.saman.eventhub.zk.ZkUtils
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CoordinatorNode(zkUtils: ZkUtils, db: DBAccess) {

  val real_time_nodes_status = "/status/real-time-nodes"
  val historical_nodes_status = "/status/historical-nodes"
  val real_time_nodes_new_tasks = "/tasks/real-time-nodes/new-tasks"
  val real_time_nodes_remove_tasks = "/tasks/real-time-nodes/remove-data"
  val historical_nodes_new_tasks = "/tasks/historical-nodes/new-tasks"
  val historical_nodes_remove_tasks = "/tasks/historical-nodes/remove-data"

  println(s"============> ${this.getClass.getName} started on ${get("server.address")}")

  def start(topics: List[String]): Unit = {

    zkUtils.createIfNotExists(real_time_nodes_status, () => null)
    zkUtils.createIfNotExists(historical_nodes_status, () => null)
    zkUtils.createIfNotExists(real_time_nodes_new_tasks, () => null)
    zkUtils.createIfNotExists(real_time_nodes_remove_tasks, () => null)
    zkUtils.createIfNotExists(historical_nodes_new_tasks, () => null)
    zkUtils.createIfNotExists(historical_nodes_remove_tasks, () => null)

    zkUtils.setData(real_time_nodes_new_tasks, () => topics.mkString(",").getBytes())

    zkUtils.watch(real_time_nodes_status, new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
        if (event.getType.equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
          if (event.getData.getData != null) {
            var sources = new mutable.HashSet[String]()
            val readSources = zkUtils.read(real_time_nodes_new_tasks, data => {
              val allSources = new String(data)
              allSources.split(",").toSet
            })
            readSources match {
              case Some(_sources) => sources = sources union _sources
              case None => {}
            }
            val data = new String(event.getData.getData)
            for (segment <- data.split(",")) {
              zkUtils.deletePath(s"/segments/$segment")
              sources.add(segment.split("/")(0))
            }
            zkUtils.setData(real_time_nodes_new_tasks, () => sources.mkString(",").getBytes())
          }
        }
      }
    })

    zkUtils.watch(historical_nodes_status, new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
        if (event.getType.equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
          if (event.getData.getData != null) {
            val data = new String(event.getData.getData)
            val toBeReplayed = data.split(",").toSet
            if (!toBeReplayed.isEmpty) {
              for (segment <- toBeReplayed)
                zkUtils.deletePath(s"/segments/$segment")
              updateTasksForHistoricalNodes(toBeReplayed)
            }
          }
        }
      }
    })

    Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
      override def run() = {
        val toSync = db.read("select * from segments where is_loaded_to_historical_nodes = false", rs => {
          var listBuffer = new ListBuffer[String]()
          while (rs.next()) {
            val source = rs.getString("source")
            val epoch_from = rs.getLong("epoch_from")
            val epoch_to = rs.getLong("epoch_to")
            val last_updated = rs.getTimestamp("last_updated").getTime
            listBuffer += s"$source/$epoch_from-$epoch_to"
          }
          listBuffer.toSet
        })
        if (!toSync.isEmpty) {
          updateTasksForHistoricalNodes(toSync)
        }
      }
    }, 10000, 10000, TimeUnit.MILLISECONDS)
  }

  def updateTasksForHistoricalNodes(segments: Set[String]) = {
    val readSegments = zkUtils.read(historical_nodes_new_tasks, data => {
      val allSegments = new String(data)
      allSegments.split(",").toSet
    })
    val allSegments = if (readSegments.isDefined) {
      readSegments.get union segments
    } else {
      segments
    }
    zkUtils.setData(historical_nodes_new_tasks, () => allSegments.mkString(",").getBytes())
  }
}
