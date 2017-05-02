package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.advancedtelematic.campaigner.client.Director
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.Namespace
import scala.concurrent.ExecutionContext

object GroupScheduler {

  final case class LaunchBatch()
  final case class BatchComplete(grp: GroupId, offset: Int)
  final case class GroupComplete(grp: GroupId)

  def props(registry: DeviceRegistry,
            director: Director,
            batchSize: Int,
            ns: Namespace,
            update: UpdateId,
            grp: GroupId)
           (implicit ec: ExecutionContext): Props =
    Props(new GroupScheduler(registry, director, batchSize, ns, update, grp))
}

class GroupScheduler(registry: DeviceRegistry,
                     director: Director,
                     batchSize: Int,
                     ns: Namespace,
                     update: UpdateId,
                     grp: GroupId)
  (implicit ec: ExecutionContext) extends Actor {

  import GroupScheduler._
  import context._
  import scala.util.{Failure, Success}

  val log = Logging(system, this)
  val scheduler = system.scheduler

  def processing(offset: Int): Receive = {
    case LaunchBatch() =>
      log.debug(s"scheduling group $grp from $offset to ${offset + batchSize}")
      registry.getDevicesInGroup(ns, grp, offset, batchSize).flatMap(
        director.setMultiUpdateTarget(ns, update, _)
      ) onComplete {
        case Success(c) if c.length == 0 || c.length < batchSize =>
          log.debug(s"group $grp complete")
          parent ! GroupComplete(grp)
        case Success(_) =>
          log.debug(s"batch for $grp from $offset to ${offset + batchSize} complete")
          become(processing(offset + batchSize))
          parent ! BatchComplete(grp, offset)
        case Failure(err) =>
          log.error(err.toString)
      }
    case msg => log.info(s"unexpected message: $msg")
  }

  def receive: Receive = {
    case LaunchBatch() =>
      become(processing(0))
      self ! LaunchBatch()
    case msg => log.info(s"unexpected message: $msg")
  }

}
