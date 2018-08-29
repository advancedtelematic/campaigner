package com.advancedtelematic.campaigner.db

import cats.syntax.option._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType.DeviceStatus.DeviceStatus
import com.advancedtelematic.campaigner.data.DataType.GroupStatus.GroupStatus
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.SlickMapping._
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import com.advancedtelematic.libats.slick.db.SlickExtensions._
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

object Campaigns {
  def apply()(implicit db: Database, ec: ExecutionContext): Campaigns = new Campaigns()
}

// TODO:SM Abstract group stats code to separate class?
protected [db] class Campaigns(implicit db: Database, ec: ExecutionContext)
  extends GroupStatsSupport
    with CampaignSupport
    with CampaignMetadataSupport
    with DeviceUpdateSupport
    with CancelTaskSupport {

  val campaignStatusTransition = new CampaignStatusTransition()

  def remainingCancelling(): Future[Seq[(Namespace, CampaignId)]] = cancelTaskRepo.findInprogress()

  def remainingCampaigns(): Future[Seq[Campaign]] = campaignRepo.findAllScheduled()

  def remainingGroups(campaign: CampaignId): Future[Seq[GroupId]] =
    groupStatsRepo.findScheduled(campaign).map(_.map(_.group))

  def remainingBatches(campaign: CampaignId, group: GroupId): Future[Option[Long]] =
    groupStatsRepo.findScheduled(campaign, Some(group))
      .map(_.headOption)
      .map(_.map(_.processed))

  def completeBatch(campaignId: CampaignId, group: GroupId, stats: Stats): Future[Unit] = db.run {
    val io = for {
      _ <- progressGroupAction(campaignId, group, GroupStatus.scheduled, stats)
      _ <- campaignStatusTransition.groupOrBatchCompleted(campaignId)
    } yield ()

    io.transactionally
  }

  def completeGroup(campaign: CampaignId, group: GroupId, stats: Stats): Future[Unit] = db.run {
    val io = for {
      _ <- progressGroupAction(campaign, group, GroupStatus.launched, stats)
      _ <- campaignStatusTransition.groupOrBatchCompleted(campaign)
    } yield ()

    io.transactionally
  }

  def cancelCampaign(campaign: CampaignId): Future[Unit] = db.run {
    val io = for {
      _ <- campaignRepo.findAction(campaign)
      _ <- cancelTaskRepo.cancelAction(campaign)
      _ <- groupStatsRepo.cancelAction(campaign)
      _ <- campaignStatusTransition.cancel(campaign)
    } yield ()

    io.transactionally
  }

  def failedDevices(campaign: CampaignId): Future[Set[DeviceId]] = db.run {
    campaignRepo.findAction(campaign).flatMap { _ =>
      deviceUpdateRepo.findByCampaignAction(campaign, DeviceStatus.failed)
    }
  }

  def freshCancelled(): Future[Seq[(Namespace, CampaignId)]] =
    cancelTaskRepo.findPending()

  def freshCampaigns(): Future[Seq[Campaign]] =
    campaignRepo.findAllScheduled { groupStats =>
      groupStats.processed === 0L && groupStats.affected === 0L
    }

  def scheduleDevices(campaign: CampaignId, update: UpdateId, devices: DeviceId*): Future[Unit] =
    deviceUpdateRepo.persistMany(devices.map { d => DeviceUpdate(campaign, update, d, DeviceStatus.scheduled) })

  def markDevicesAccepted(campaign: CampaignId, update: UpdateId, devices: DeviceId*): Future[Unit] =
    deviceUpdateRepo.persistMany(devices.map { d => DeviceUpdate(campaign, update, d, DeviceStatus.accepted) })

  def finishDevice(updateId: UpdateId, device: DeviceId, status: DeviceStatus): Future[Unit] = db.run {
    for {
      _ <- deviceUpdateRepo.setUpdateStatusAction(updateId, device, status)
      campaigns <- campaignRepo.findByUpdateAction(updateId)
      _ <- DBIO.sequence(campaigns.map(c => campaignStatusTransition.deviceFinished(c.id)))
    } yield ()
  }

  def finishDevices(campaign: CampaignId, devices: Seq[DeviceId], status: DeviceStatus): Future[Unit] =
    deviceUpdateRepo.setUpdateStatus(campaign, devices, status)

  def countFinished(campaignId: CampaignId): Future[Long] =
    campaignRepo.countDevices(campaignId) { status =>
      status === DeviceStatus.successful || status === DeviceStatus.failed
    }

  def countCancelled(campaignId: CampaignId): Future[Long] =
    campaignRepo.countDevices(campaignId) { status =>
      status === DeviceStatus.cancelled
    }

  def allCampaigns(ns: Namespace, offset: Long, limit: Long, status: Option[CampaignStatus]): Future[PaginationResult[CampaignId]] =
    campaignRepo.all(ns, offset, limit, status)

  def findNamespaceCampaign(ns: Namespace, campaignId: CampaignId): Future[Campaign] =
    campaignRepo.find(campaignId, Option(ns))

  def findClientCampaign(campaignId: CampaignId): Future[GetCampaign] = for {
    c <- campaignRepo.find(campaignId)
    groups <- db.run(findGroupsAction(c.id))
    metadata <- campaignMetadataRepo.findFor(campaignId)
  } yield GetCampaign(c, groups, metadata)

  def findCampaignsByUpdate(update: UpdateId): Future[Seq[Campaign]] =
    db.run(campaignRepo.findByUpdateAction(update))

  def campaignStats(campaignId: CampaignId): Future[CampaignStats] = for {
    status    <- findClientCampaign(campaignId).map(_.status)
    finished  <- countFinished(campaignId)
    cancelled <- countCancelled(campaignId)
    failed    <- failedDevices(campaignId)
    stats     <- campaignStatsFor(campaignId)
  } yield CampaignStats(campaignId, status, finished, failed, cancelled, stats)

  def launch(id: CampaignId): Future[Unit] = db.run {
   val io = for {
      groups <- findGroupsAction(id)
      _ <- scheduleGroupsAction(id, groups)
      _ <- campaignStatusTransition.launch(id)
    } yield ()

    io.transactionally
  }

  def create(campaign: Campaign, groups: Set[GroupId], metadata: Seq[CampaignMetadata]): Future[CampaignId] =
    campaignRepo.persist(campaign, groups, metadata)

  def update(id: CampaignId, name: String, metadata: Seq[CampaignMetadata]): Future[Unit] =
    campaignRepo.update(id, name, metadata)

  protected [db] def scheduleGroupsAction(campaignId: CampaignId, groups: Set[GroupId]): DBIO[Unit] =
    campaignRepo
      .findAction(campaignId)
      .andThen(groupStatsRepo.persistManyAction(campaignId, groups))
      .transactionally
      .handleIntegrityErrors(Errors.CampaignAlreadyLaunched)

  def scheduleGroups(campaign: CampaignId, groups: Set[GroupId]): Future[Unit] =
    db.run(scheduleGroupsAction(campaign, groups))

  def campaignStatsFor(campaign: CampaignId): Future[Map[GroupId, Stats]] =
    db.run {
      campaignRepo.findAction(campaign).flatMap { _ =>
        groupStatsRepo.findByCampaignAction(campaign)
      }
    }.map {
      _.groupBy(_.group).map {
        case (group, stats +: _) => (group, Stats(stats.processed, stats.affected))
      }
    }

  private def findGroupsAction(campaignId: CampaignId): DBIO[Set[GroupId]] =
    campaignRepo.findAction(campaignId).flatMap { _ =>
        Schema.campaignGroups
          .filter(_.campaignId === campaignId)
          .map(_.groupId)
          .result
          .map(_.toSet)
    }

  private def progressGroupAction(campaignId: CampaignId, group: GroupId, status: GroupStatus, stats: Stats): DBIO[Unit] =
    if (stats.affected > stats.processed)
      DBIO.failed(Errors.InvalidCounts)
    else
      groupStatsRepo.updateGroupStatsAction(campaignId, group, status, stats)
}

protected [db] class CampaignStatusTransition(implicit db: Database, ec: ExecutionContext) extends CampaignSupport
  with GroupStatsSupport  {

  def groupOrBatchCompleted(campaignId: CampaignId): DBIO[Unit] =
    updateToCalculatedStatus(campaignId)

  def deviceFinished(campaignId: CampaignId): DBIO[Unit] =
    updateToCalculatedStatus(campaignId)

  def launch(campaignId: CampaignId): DBIO[CampaignId] =
    campaignRepo.setStatusAction(campaignId, CampaignStatus.launched)

  def cancel(campaignId: CampaignId): DBIO[CampaignId] =
    campaignRepo.setStatusAction(campaignId, CampaignStatus.cancelled)

  def updateToCalculatedStatus(campaignId: CampaignId): DBIO[Unit] =
    for {
      maybeStatus <- calculateCampaignStatus(campaignId)
      _ <-  maybeStatus match {
        case Some(status) =>
          campaignRepo.setStatusAction(campaignId, status)
        case None =>
          DBIO.successful(())
      }
    } yield ()

  private def calculateCampaignStatus(campaign: CampaignId): DBIO[Option[CampaignStatus]] = {
    def countDevicesByStatus(status: GroupStatus) =
      Schema.groupStats.filter(_.campaignId === campaign).filter(_.status === status).length.result

    val countAffectedDevices =
      Schema.groupStats.filter(_.campaignId === campaign).map(_.affected).sum.result.map(_.getOrElse(0L))

    val countFinishedDevices =
      Schema.deviceUpdates
        .filter(_.campaignId === campaign).map(_.status)
        .filter(status => status === DeviceStatus.successful || status === DeviceStatus.failed)
        .length.result

    for {
      groupCount <- groupStatsRepo.findByCampaignAction(campaign).map(_.length)
      scheduled <- countDevicesByStatus(GroupStatus.scheduled)
      cancelled <- countDevicesByStatus(GroupStatus.cancelled)

      affected <- countAffectedDevices
      finished <- countFinishedDevices
      status = (groupCount, scheduled, cancelled, affected, finished) match {
        case (_, _, c, _, _) if c > 0           => CampaignStatus.cancelled.some
        case (g, 0, _, a, f) if g > 0 && a == f => CampaignStatus.finished.some
        case _                                  => None
      }
    } yield status
  }
}
