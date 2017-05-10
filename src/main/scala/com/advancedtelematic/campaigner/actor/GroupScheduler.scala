package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client.Director
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.Namespace
import scala.concurrent.ExecutionContext

object GroupScheduler {

  final case class NextBatch()
  final case class StartBatch(offset: Int)
  final case class BatchComplete(grp: GroupId, stats: Stats)
  final case class GroupComplete(grp: GroupId, stats: Stats)

  def props(registry: DeviceRegistry,
            director: Director,
            ns: Namespace,
            update: UpdateId,
            grp: GroupId)
           (implicit ec: ExecutionContext): Props =
    Props(new GroupScheduler(registry, director, ns, update, grp))
}

class GroupScheduler(registry: DeviceRegistry,
                     director: Director,
                     ns: Namespace,
                     update: UpdateId,
                     grp: GroupId)
  (implicit ec: ExecutionContext) extends Actor with Settings {

  import GroupScheduler._
  import context._
  import scala.util.{Failure, Success}

  val log = Logging(system, this)
  val scheduler = system.scheduler

  def processing(offset: Int): Receive = {
    case NextBatch() =>
      log.debug(s"scheduling group $grp from $offset to ${offset + schedulerBatchSize}")
      (for {
        processed <- registry.getDevicesInGroup(ns, grp, offset, schedulerBatchSize)
        affected  <- director.setMultiUpdateTarget(ns, update, processed)
      } yield (processed, affected)) onComplete {
        case Success((p, a)) if p.length < schedulerBatchSize =>
          log.debug(s"group $grp complete")
          parent ! GroupComplete(grp, Stats(p.length, a.length))
          context stop self
        case Success((p, a)) =>
          log.debug(s"batch for $grp from $offset to ${offset + schedulerBatchSize} complete")
          become(processing(offset + schedulerBatchSize))
          parent ! BatchComplete(grp, Stats(p.length, a.length))
        case Failure(err) =>
          log.error(err.toString)
      }
    case msg => log.info(s"unexpected message: $msg")
  }

  def receive: Receive = {
    case StartBatch(offset) =>
      become(processing(0))
      self ! NextBatch()
    case msg => log.info(s"unexpected message: $msg")
  }

}
