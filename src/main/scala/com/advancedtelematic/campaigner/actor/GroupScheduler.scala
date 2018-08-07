package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, Props, Status}
import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId

import scala.concurrent.duration._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Future

object GroupScheduler {

  sealed trait GroupSchedulerMsg
  object NextBatch extends GroupSchedulerMsg
  case class BatchComplete(group: GroupId, offset: Long) extends GroupSchedulerMsg
  case class GroupComplete(group: GroupId) extends GroupSchedulerMsg

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
    result <- scheduleAffectedOnly(campaign, offset, nextBatchDevices)
  } yield result

  private def scheduleDevices(devs: Seq[DeviceId]) =
    Future.traverse(devs)(campaigns.scheduleDevice(campaign.id, campaign.updateId, _))

  private def markAccepted(devs: Seq[DeviceId])=
    Future.traverse(devs)(campaigns.markDeviceAccepted(campaign.id, campaign.updateId, _))

  def scheduleAffectedOnly(campaign: Campaign, offset: Long, groupDevices: Seq[DeviceId]): Future[GroupSchedulerMsg] = {
    val updateStatusF =
      if(campaign.autoAccept)
        for {
          affected <- director.setMultiUpdateTarget(campaign.namespace, campaign.updateId, groupDevices)
          _ <- markAccepted(affected)
        } yield affected
      else {
        for {
          affected <- director.findAffected(campaign.namespace, campaign.updateId, groupDevices)
          _ <- scheduleDevices(affected)
        } yield affected
      }

    updateStatusF.flatMap { affected => scheduleBatch(offset, groupDevices.length.toLong, affected) }
  }

  private def scheduleBatch(offset: Long, processed: Long, affected: Seq[DeviceId]): Future[GroupSchedulerMsg] = {
    if(processed < batchSize) {
      log.debug(s"$group complete")
      val stats = Stats(processed, affected.length.toLong)
      campaigns.completeGroup(campaign.namespace, campaign.id, group, stats)
        .map(_ => GroupComplete(group))
    } else {
      val frontier = offset + processed
      log.debug(s"batch complete for $group from $offset to $frontier")
      campaigns.completeBatch(campaign.namespace, campaign.id, group, Stats(frontier, affected.length.toLong))
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
      context.stop(self)

    case msg: Status.Failure =>
      parent ! msg
  }
}
