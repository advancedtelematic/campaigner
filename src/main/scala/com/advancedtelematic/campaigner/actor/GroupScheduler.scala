package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, Props}
import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId

import scala.concurrent.duration._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Future

object GroupScheduler {

  private object NextBatch
  private final case class ScheduleBatch(offset: Long, processed: Long, affected: Seq[DeviceId])
  final case class BatchComplete(group: GroupId, offset: Long)
  final case class GroupComplete(group: GroupId)
  private final case class Error(msg: String, error: Throwable)

  def props(registry: DeviceRegistryClient,
            director: DirectorClient,
            delay: FiniteDuration,
            batchSize: Long,
            campaign: Campaign,
            group: GroupId)
           (implicit db: Database): Props =
    Props(new GroupScheduler(registry, director, delay, batchSize, campaign, group))
}

class GroupScheduler(registry: DeviceRegistryClient,
                     director: DirectorClient,
                     delay: FiniteDuration,
                     batchSize: Long,
                     campaign: Campaign,
                     group: GroupId)
                    (implicit db: Database) extends Actor
  with ActorLogging {

  import GroupScheduler._
  import akka.pattern._
  import context._

  val scheduler = system.scheduler

  val campaigns = Campaigns()

  override def preStart() =
    self ! NextBatch

  private def resumeBatch(campaign: Campaign, group: GroupId): Future[ScheduleBatch] = for {
    offsetOpt <- campaigns.remainingBatches(campaign.id, group)
    offset     = offsetOpt.getOrElse(0L)
    // TODO: This offset is not currently stable. We are not sorting the devices on device registry before pagination
    processed <- registry.devicesInGroup(campaign.namespace, group, offset, batchSize)
    affected  <- director.setMultiUpdateTarget(campaign.namespace, campaign.updateId, processed)
  } yield ScheduleBatch(offset, processed.length.toLong, affected)

  private def scheduleDevices(devs: Seq[DeviceId]): Future[Unit] =
    Future.traverse(devs)(campaigns.scheduleDevice(campaign.id, campaign.updateId, _))
      .map(_ => ())

  def receive: Receive = {
    case NextBatch =>
      log.debug(s"next batch")
      resumeBatch(campaign, group)
        .recover { case err => Error("could not resume batch", err) }
        .pipeTo(self)
    case ScheduleBatch(_, processed, affected) if processed < batchSize => // last batch
      log.debug(s"$group complete")
      val stats = Stats(processed, affected.length.toLong)
      scheduleDevices(affected)
        .flatMap { _ =>
          campaigns.completeGroup(campaign.namespace, campaign.id, group, stats)
            .map(_ => GroupComplete(group))
            .recover { case err => Error("could not persist progress", err) }
        }
        .recover { case err => Error("could not schedule devices", err) }
        .pipeTo(parent)
        .andThen { case _ => context.stop(self) }
    case ScheduleBatch(offset, processed, affected) =>
      val frontier = offset + processed
      log.debug(s"batch complete for $group from $offset to $frontier")
      scheduleDevices(affected)
        .flatMap { _ =>
          campaigns.completeBatch(campaign.namespace, campaign.id, group, Stats(frontier, affected.length.toLong))
            .map(_ => BatchComplete(group, frontier))
            .recover { case err => Error("could not complete batch", err) }
        }
        .recover { case err => Error("could not schedule devices", err) }
        .andThen { case _ => scheduler.scheduleOnce(delay, self, NextBatch)}
        .pipeTo(parent)
    case Error(msg, err) => log.error(s"$msg: ${err.getMessage}")
  }

}
