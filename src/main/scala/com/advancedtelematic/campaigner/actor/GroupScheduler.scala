package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, Props, Status}
import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.{Campaigns, DeviceUpdateProcess}
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId

import scala.concurrent.duration._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Future

object GroupScheduler {

  sealed trait GroupSchedulerMsg
  object NextBatch extends GroupSchedulerMsg
  case class BatchComplete(group: GroupId, offset: Long) extends GroupSchedulerMsg
  case class GroupComplete(group: GroupId) extends GroupSchedulerMsg
  case class GroupSchedulerError(cause: Throwable) extends GroupSchedulerMsg

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
                     group: GroupId)(implicit db: Database) extends Actor with ActorLogging {

  import GroupScheduler._
  import akka.pattern._
  import context._

  val scheduler = system.scheduler

  val campaigns = Campaigns()

  val deviceUpdateProcess = new DeviceUpdateProcess(director)

  override def preStart() =
    self ! NextBatch

  private def findNextBatchDevices(campaign: Campaign, group: GroupId): Future[(Long, Seq[DeviceId])] = for {
    offsetOpt <- campaigns.remainingBatches(campaign.id, group)
    offset = offsetOpt.getOrElse(0L)
    // TODO: This offset is not currently stable. We are not sorting the devices on device registry before pagination
    processed <- registry.devicesInGroup(campaign.namespace, group, offset, batchSize)
  } yield (offset, processed)

  private def resumeBatch(campaign: Campaign, group: GroupId): Future[GroupSchedulerMsg] = for {
    (offset, nextBatchDevices) <- findNextBatchDevices(campaign, group)
    result <- startAffectedDevices(campaign, offset, nextBatchDevices)
  } yield result

  private def startAffectedDevices(campaign: Campaign, offset: Long, groupDevices: Seq[DeviceId]): Future[GroupSchedulerMsg] =
    deviceUpdateProcess.startUpdateFor(groupDevices, campaign).flatMap { affected =>
      completeBatch(offset, groupDevices.length.toLong, affected)
    }

  private def completeBatch(offset: Long, processed: Long, affected: Seq[DeviceId]): Future[GroupSchedulerMsg] = {
    if(processed < batchSize) {
      log.debug(s"$group complete")
      val stats = Stats(processed, affected.length.toLong)
      campaigns.completeGroup(campaign.id, group, stats)
        .map(_ => GroupComplete(group))
    } else {
      val frontier = offset + processed
      log.debug(s"batch complete for $group from $offset to $frontier")
      campaigns.completeBatch(campaign.id, group, Stats(frontier, affected.length.toLong))
        .map(_ => BatchComplete(group, frontier))
    }
  }

  def receive: Receive = {
    case NextBatch =>
      log.debug(s"next batch")
      resumeBatch(campaign, group).pipeTo(self)

    case msg: BatchComplete =>
      parent ! msg

      scheduler.scheduleOnce(delay, self, NextBatch)

    case msg: GroupComplete =>
      parent ! msg

    case Status.Failure(cause) =>
      log.error(cause, "Could not schedule group batch")
      parent ! GroupSchedulerError(cause)
      throw cause
  }
}
