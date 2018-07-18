package com.advancedtelematic.campaigner.db

import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType.DeviceStatus.DeviceStatus
import com.advancedtelematic.campaigner.data.DataType.GroupStatus.GroupStatus
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import com.advancedtelematic.libats.slick.db.SlickExtensions._
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._

import scala.concurrent.{ExecutionContext, Future}
import slick.jdbc.MySQLProfile.api._
import SlickMapping._

object Campaigns {
  def apply()(implicit db: Database, ec: ExecutionContext): Campaigns = new Campaigns()
}

protected [db] class Campaigns(implicit db: Database, ec: ExecutionContext)
  extends GroupStatsSupport
    with CampaignSupport
    with CampaignMetadataSupport
    with DeviceUpdateSupport
    with CancelTaskSupport {

  def remainingCancelling(): Future[Seq[(Namespace, CampaignId)]] = cancelTaskRepo.findInprogress()

  def remainingCampaigns(): Future[Seq[Campaign]] = campaignRepo.findAllScheduled()

  def remainingGroups(campaign: CampaignId): Future[Seq[GroupId]] =
    groupStatsRepo.findScheduled(campaign).map(_.map(_.group))

  def remainingBatches(campaign: CampaignId, group: GroupId): Future[Option[Long]] =
    groupStatsRepo.findScheduled(campaign, Some(group))
      .map(_.headOption)
      .map(_.map(_.processed))

  def completeBatch(
    ns: Namespace,
    campaign: CampaignId,
    group: GroupId,
    stats: Stats): Future[Unit] =
    progressGroup(ns, campaign, group, GroupStatus.scheduled, stats)

  def completeGroup(
    ns: Namespace,
    campaign: CampaignId,
    group: GroupId,
    stats: Stats): Future[Unit] =
    progressGroup(ns, campaign, group, GroupStatus.launched, stats)

  def cancelCampaign(
    ns: Namespace,
    campaign: CampaignId): Future[Unit] = for {
      _ <- campaignRepo.find(ns, campaign)
      _ <- cancelTaskRepo.cancel(campaign)
      _ <- groupStatsRepo.cancel(campaign)
    } yield ()

  def failedDevices(ns: Namespace, campaign: CampaignId): Future[Set[DeviceId]] = db.run {
    campaignRepo.findAction(ns, campaign).flatMap { _ =>
      deviceUpdateRepo.findByCampaignAction(campaign, DeviceStatus.failed)
    }
  }

  def scheduledDevices(ns: Namespace, campaign: CampaignId): Future[Set[DeviceId]] = db.run {
    campaignRepo.findAction(ns, campaign).flatMap { _ =>
      deviceUpdateRepo.findByCampaignAction(campaign, DeviceStatus.scheduled)
    }
  }

  def freshCancelled(): Future[Seq[(Namespace, CampaignId)]] =
    cancelTaskRepo.findPending()

  def freshCampaigns(): Future[Seq[Campaign]] =
    campaignRepo.findAllScheduled { groupStats =>
      groupStats.processed === 0L && groupStats.affected === 0L
    }

  def scheduleDevice(campaign: CampaignId, update: UpdateId, device: DeviceId): Future[Unit] =
    deviceUpdateRepo.persist(DeviceUpdate(campaign, update, device, DeviceStatus.scheduled))

  def finishDevice(update: UpdateId, device: DeviceId, status: DeviceStatus): Future[Unit] =
    deviceUpdateRepo.setUpdateStatus(update, device, status)

  def finishDevices(campaign: CampaignId, devices: Seq[DeviceId], status: DeviceStatus): Future[Unit] =
    deviceUpdateRepo.setUpdateStatus(campaign, devices, status)

  def countFinished(ns: Namespace, campaignId: CampaignId): Future[Long] =
    campaignRepo.countDevices(ns, campaignId) { status =>
      status === DeviceStatus.successful || status === DeviceStatus.failed
    }

  def countCancelled(ns: Namespace, campaignId: CampaignId): Future[Long] =
    campaignRepo.countDevices(ns, campaignId) { status =>
      status === DeviceStatus.cancelled
    }

  def allCampaigns(ns: Namespace, offset: Long, limit: Long): Future[PaginationResult[CampaignId]] =
    campaignRepo.all(ns, offset, limit)

  def findCampaign(ns: Namespace, campaignId: CampaignId): Future[GetCampaign] = for {
    c <- campaignRepo.find(ns, campaignId)
    groups <- findGroups(ns, c.id)
    metadata <- campaignMetadataRepo.findFor(campaignId)
  } yield GetCampaign(c, groups, metadata)

  def findCampaignsByUpdate(ns: Namespace, update: UpdateId): Future[Seq[CampaignId]] =
    db.run(campaignRepo.findByUpdateAction(ns, update))

  def status(campaign: CampaignId): Future[CampaignStatus] =
    groupStatsRepo.aggregatedStatus(campaign)

  def campaignStats(ns: Namespace, campaign: CampaignId): Future[CampaignStats] = for {
    status    <- status(campaign)
    finished  <- countFinished(ns, campaign)
    cancelled <- countCancelled(ns, campaign)
    failed    <- failedDevices(ns, campaign)
    stats     <- campaignStatsFor(ns, campaign)
  } yield CampaignStats(campaign, status, finished, failed, cancelled, stats)

  def launch(ns: Namespace, id: CampaignId): Future[Unit] = for {
    groups <- findGroups(ns, id)
    _ <- scheduleGroups(ns, id, groups)
  } yield ()

  def create(campaign: Campaign, groups: Set[GroupId], metadata: Seq[CampaignMetadata]): Future[CampaignId] =
    campaignRepo.persist(campaign, groups, metadata)

  def update(ns: Namespace, id: CampaignId, name: String): Future[Unit] =
    campaignRepo.updateName(ns, id, name)

  def scheduleGroups(ns: Namespace, campaign: CampaignId, groups: Set[GroupId]): Future[Unit] =
    db.run {
      campaignRepo
        .findAction(ns, campaign)
        .andThen(groupStatsRepo.persistManyAction(campaign, groups))
        .transactionally
        .handleIntegrityErrors(Errors.CampaignAlreadyLaunched)
    }

  def campaignStatsFor(ns: Namespace, campaign: CampaignId): Future[Map[GroupId, Stats]] =
    db.run {
      campaignRepo.findAction(ns, campaign).flatMap { _ =>
        groupStatsRepo.findByCampaignAction(campaign)
      }
    }.map {
      _.groupBy(_.group).map {
        case (group, stats +: _) => (group, Stats(stats.processed, stats.affected))
      }
    }

  // TODO: I think this can go, see schema.scala
  private def findGroups(ns: Namespace, campaign: CampaignId): Future[Set[GroupId]] =
    db.run {
      campaignRepo.findAction(ns, campaign).flatMap { _ =>
        Schema.campaignGroups
          .filter(_.campaignId === campaign)
          .map(_.groupId)
          .result
          .map(_.toSet)
      }
    }

  private[db] def progressGroup(ns: Namespace,
                                campaign: CampaignId,
                                group: GroupId,
                                status: GroupStatus,
                                stats: Stats): Future[Unit] =
    if (stats.affected > stats.processed)
      Future.failed(Errors.InvalidCounts)
    else
      groupStatsRepo.updateGroupStats(campaign, group, status, stats)
}
