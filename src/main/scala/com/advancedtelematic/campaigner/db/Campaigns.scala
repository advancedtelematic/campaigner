package com.advancedtelematic.campaigner.db

import cats.data.NonEmptyList
import cats.syntax.either._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType.DeviceStatus.DeviceStatus
import com.advancedtelematic.campaigner.data.DataType.GroupStatus.GroupStatus
import com.advancedtelematic.campaigner.data.DataType.SortBy.SortBy
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.SlickMapping._
import com.advancedtelematic.campaigner.http.Errors._
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

protected [db] class Campaigns(implicit db: Database, ec: ExecutionContext)
  extends GroupStatsSupport
    with CampaignSupport
    with CampaignMetadataSupport
    with DeviceUpdateSupport
    with CancelTaskSupport {

  val campaignStatusTransition = new CampaignStatusTransition()

  def remainingCancelling(): Future[Seq[(Namespace, CampaignId)]] = cancelTaskRepo.findInprogress()

  /**
   * Returns all campaigns that have devices in `requested` state
   */
  def remainingCampaigns(): Future[Set[Campaign]] =
    db.run(campaignRepo.findAllWithRequestedDevices)

  def remainingGroups(campaign: CampaignId): Future[Seq[GroupId]] =
    groupStatsRepo.findScheduled(campaign).map(_.map(_.group))

  def remainingBatches(campaign: CampaignId, group: GroupId): Future[Option[Long]] =
    groupStatsRepo.findScheduled(campaign, Some(group))
      .map(_.headOption)
      .map(_.map(_.processed))

  /**
   * Given a campaign ID, returns IDs of all devices that are in `requested`
   * state
   */
  def requestedDevices(campaign: CampaignId): Future[Set[DeviceId]] =
    deviceUpdateRepo.findByCampaign(campaign, DeviceStatus.requested)

  def completeBatch(campaignId: CampaignId, group: GroupId, stats: Stats): Future[Unit] = db.run {
    campaignStatusTransition.completeBatch(campaignId, group, stats).transactionally
  }

  def completeGroup(campaign: CampaignId, group: GroupId, stats: Stats): Future[Unit] = db.run {
    campaignStatusTransition.completeGroup(campaign, group, stats).transactionally
  }

  /**
   * Re-calculates the status of the campaign and updates the table
   */
  def updateStatus(campaignId: CampaignId): Future[Unit] =
    db.run(campaignStatusTransition.updateToCalculatedStatus(campaignId))

  /**
   * Given a campaign ID, returns IDs of all devices that are in `failed` state
   */
  private def findFailedDevicesAction(campaign: CampaignId): DBIO[Set[DeviceId]] =
    campaignRepo.findAction(campaign).flatMap { _ =>
      deviceUpdateRepo.findByCampaignAction(campaign, DeviceStatus.failed)
    }

  def freshCancelled(): Future[Seq[(Namespace, CampaignId)]] =
    cancelTaskRepo.findPending()

  /**
   * Returns all newly created, not yet scheduled campaigns
   */
  def freshCampaigns(): Future[Set[Campaign]] =
    db.run(campaignRepo.findAllNewlyCreated)

  /**
   * Sets status of each given device to `rejected` for a given campaign and
   * update.
   */
  def rejectDevices(campaignId: CampaignId, updateId: UpdateId, deviceIds: Seq[DeviceId]): Future[Unit] =
    deviceUpdateRepo.persistMany(deviceIds.map(deviceId =>
      DeviceUpdate(campaignId, updateId, deviceId, DeviceStatus.rejected)
    ))

  def scheduleDevices(campaign: CampaignId, update: UpdateId, devices: DeviceId*): Future[Unit] =
    deviceUpdateRepo.persistMany(devices.map { d => DeviceUpdate(campaign, update, d, DeviceStatus.scheduled) })

  def markDevicesAccepted(campaign: CampaignId, update: UpdateId, devices: DeviceId*): Future[Unit] =
    deviceUpdateRepo.persistMany(devices.map { d => DeviceUpdate(campaign, update, d, DeviceStatus.accepted) })

  def succeedDevice(updateId: UpdateId, deviceId: DeviceId, successCode: String): Future[Unit] =
    finishDevice(updateId, deviceId, DeviceStatus.successful, Some(successCode))

  def succeedDevices(campaignId: CampaignId, devices: Seq[DeviceId], successCode: String): Future[Unit] =
    finishDevices(campaignId, devices, DeviceStatus.successful, Some(successCode))

  def failDevice(updateId: UpdateId, deviceId: DeviceId, failureCode: String): Future[Unit] =
    finishDevice(updateId, deviceId, DeviceStatus.failed, Some(failureCode))

  def failDevices(campaignId: CampaignId, devices: Seq[DeviceId], failureCode: String): Future[Unit] =
    finishDevices(campaignId, devices, DeviceStatus.failed, Some(failureCode))

  def cancelDevice(updateId: UpdateId, deviceId: DeviceId): Future[Unit] =
    finishDevice(updateId, deviceId, DeviceStatus.cancelled, None)

  def cancelDevices(campaignId: CampaignId, devices: Seq[DeviceId]): Future[Unit] =
    finishDevices(campaignId, devices, DeviceStatus.cancelled, None)

  private def finishDevice(updateId: UpdateId, device: DeviceId, status: DeviceStatus, resultCode: Option[String]): Future[Unit] = db.run {
    for {
      _ <- deviceUpdateRepo.setUpdateStatusAction(updateId, device, status, resultCode)
      campaigns <- campaignRepo.findByUpdateAction(updateId)
      _ <- DBIO.sequence(campaigns.map(c => campaignStatusTransition.devicesFinished(c.id)))
    } yield ()
  }

  private def finishDevices(campaignId: CampaignId, devices: Seq[DeviceId], status: DeviceStatus, resultCode: Option[String]): Future[Unit] = db.run {
    deviceUpdateRepo.setUpdateStatusAction(campaignId, devices, status, resultCode)
      .andThen(campaignStatusTransition.devicesFinished(campaignId))
  }

  def countByStatus: Future[Map[CampaignStatus, Int]] =
    db
      .run(campaignRepo.countByStatus)
      .map { counts =>
        CampaignStatus.values.map(s => s -> counts.getOrElse(s, 0)).toMap
      }

  /**
   * Returns the number of devices that took part in the given campaigns and
   * finished, either successfully or with a failure.
   */
  private def countFinishedAction(campaignIds: Set[CampaignId]): DBIO[Long] =
    campaignRepo.countDevices(campaignIds) { status =>
      status === DeviceStatus.successful || status === DeviceStatus.failed
    }

  /**
   * Returns the number of devices that took part in the given campaigns, but
   * were cancelled before the update could be applied.
   */
  private def countCancelledAction(campaignIds: Set[CampaignId]): DBIO[Long] =
    campaignRepo.countDevices(campaignIds) { status =>
      status === DeviceStatus.cancelled
    }

  def allCampaigns(ns: Namespace, sortBy: SortBy, offset: Long, limit: Long, status: Option[CampaignStatus], nameContains: Option[String]): Future[PaginationResult[CampaignId]] =
    campaignRepo.all(ns, sortBy, offset, limit, status, nameContains)

  def findNamespaceCampaign(ns: Namespace, campaignId: CampaignId): Future[Campaign] =
    campaignRepo.find(campaignId, Option(ns))

  def findClientCampaign(campaignId: CampaignId): Future[GetCampaign] = for {
    c <- campaignRepo.find(campaignId)
    retryIds <- campaignRepo.findRetryCampaignIdsOf(campaignId)
    groups <- db.run(findGroupsAction(c.id))
    metadata <- campaignMetadataRepo.findFor(campaignId)
  } yield GetCampaign(c, retryIds, groups, metadata)

  def findCampaignsByUpdate(update: UpdateId): Future[Seq[Campaign]] =
    db.run(campaignRepo.findByUpdateAction(update))

  /**
   * Calculates campaign-wide statistic counters, also taking retry campaings
   * into account if any exist.
   */
  def campaignStats(campaignId: CampaignId): Future[CampaignStats] = db.run {
    val statsAction = for {
      mainCampaign <- campaignRepo.findAction(campaignId)
      retryCampaignIds <- campaignRepo.findRetryCampaignIdsOfAction(campaignId)
      (mainProcessed, mainAffected) <- countProcessedAndAffectedDevicesForAllOfAction(Set(campaignId))
      (retryProcessed, retryAffected) <- countProcessedAndAffectedDevicesForAllOfAction(retryCampaignIds)
      mainCancelled <- countCancelledAction(Set(campaignId))
      retryCancelled <- countCancelledAction(retryCampaignIds)
      mainFinished  <- countFinishedAction(Set(campaignId))
      // TODO replace with failed devices groups when implemented
      failed <- findFailedDevicesAction(campaignId)
    } yield CampaignStats(
      campaign = campaignId,
      status = mainCampaign.status,
      finished = mainFinished - (retryProcessed - retryAffected + retryCancelled),
      failed = failed,
      cancelled = mainCancelled + retryCancelled,
      processed = mainProcessed,
      affected  = mainAffected - (retryProcessed - retryAffected)
    )

    statsAction.transactionally
  }

  def cancel(campaignId: CampaignId): Future[Unit] = db.run {
    campaignStatusTransition.cancel(campaignId)
  }

  def launch(id: CampaignId): Future[Unit] = db.run {
   val io = for {
      groups <- findGroupsAction(id)
      _ <- scheduleGroupsAction(id, groups)
      _ <- campaignStatusTransition.launch(id)
    } yield ()

    io.transactionally
  }

  def create(campaign: Campaign, groups: NonEmptyList[GroupId], devices: Set[DeviceId], metadata: Seq[CampaignMetadata]): Future[CampaignId] =
    campaignRepo.persist(campaign, groups.toList.toSet, devices, metadata)

  def update(id: CampaignId, name: String, metadata: Seq[CampaignMetadata]): Future[Unit] =
    campaignRepo.update(id, name, metadata)

  protected [db] def scheduleGroupsAction(campaignId: CampaignId, groups: Set[GroupId]): DBIO[Unit] =
    campaignRepo
      .findAction(campaignId)
      .andThen(groupStatsRepo.persistManyAction(campaignId, groups))
      .transactionally
      .handleIntegrityErrors(CampaignAlreadyLaunched)

  /**
   * Collects `processed` and `affected` counters for each group of each of the
   * given campaigns, sums them up, and returns total numbers for all the
   * campaigns
   */
  private def countProcessedAndAffectedDevicesForAllOfAction(campaignIds: Set[CampaignId]): DBIO[(Long, Long)] =
    groupStatsRepo.findByCampaignsAction(campaignIds)
      .map(_.foldLeft((0L, 0L)) { case ((totalProcessed, totalAffected), group) =>
        (totalProcessed + group.processed, totalAffected + group.affected)
      })

  private def findGroupsAction(campaignId: CampaignId): DBIO[Set[GroupId]] =
    campaignRepo.findAction(campaignId).flatMap { _ =>
        Schema.campaignGroups
          .filter(_.campaignId === campaignId)
          .map(_.groupId)
          .result
          .map(_.toSet)
    }
}

protected [db] class CampaignStatusTransition(implicit db: Database, ec: ExecutionContext) extends CampaignSupport
  with GroupStatsSupport with CancelTaskSupport  {

  def devicesFinished(campaignId: CampaignId): DBIO[Unit] =
    updateToCalculatedStatus(campaignId)

  def launch(campaignId: CampaignId): DBIO[CampaignId] =
    campaignRepo.setStatusAction(campaignId, CampaignStatus.launched)

  def cancel(campaignId: CampaignId): DBIO[Unit] =
    for {
      _ <- campaignRepo.findAction(campaignId)
      _ <- cancelTaskRepo.cancelAction(campaignId)
      _ <- groupStatsRepo.cancelAction(campaignId)
      _ <- campaignRepo.setStatusAction(campaignId, CampaignStatus.cancelled)
    } yield ()

  def completeBatch(campaignId: CampaignId, group: GroupId, stats: Stats): DBIO[Unit] = {
    for {
      _ <- progressGroupAction(campaignId, group, GroupStatus.scheduled, stats)
      _ <- updateToCalculatedStatus(campaignId)
    } yield ()
  }

  def completeGroup(campaign: CampaignId, group: GroupId, stats: Stats): DBIO[Unit] =
    for {
      _ <- progressGroupAction(campaign, group, GroupStatus.launched, stats)
      _ <- updateToCalculatedStatus(campaign)
    } yield ()

  private def progressGroupAction(campaignId: CampaignId, group: GroupId, status: GroupStatus, stats: Stats): DBIO[Unit] =
    if (stats.affected > stats.processed)
      DBIO.failed(InvalidCounts)
    else
      groupStatsRepo.updateGroupStatsAction(campaignId, group, status, stats)

  protected[db] def updateToCalculatedStatus(campaignId: CampaignId): DBIO[Unit] =
    for {
      maybeStatus <- calculateCampaignStatus(campaignId)
      _ <-  maybeStatus match {
        case Right(status) =>
          campaignRepo.setStatusAction(campaignId, status)
        case _ =>
          DBIO.successful(())
      }
    } yield ()

  protected [db] def calculateCampaignStatus(campaign: CampaignId): DBIO[Either[Unit, CampaignStatus]] = {
    def devicesWithStatus(statuses: Set[DeviceStatus]): DBIO[Long] =
      campaignRepo.countDevices(Set(campaign))(_.inSet(statuses))

    for {
      affected <- devicesWithStatus(DeviceStatus.values - DeviceStatus.requested - DeviceStatus.rejected)
      finished <- devicesWithStatus(Set(DeviceStatus.successful, DeviceStatus.failed, DeviceStatus.cancelled))
      cancelled <- devicesWithStatus(Set(DeviceStatus.cancelled))
      requested <- devicesWithStatus(Set(DeviceStatus.requested))
      total <- devicesWithStatus(DeviceStatus.values)
      status = (total, requested, cancelled, affected, finished) match {
        case (t, 0, _, a, f) if a == f => CampaignStatus.finished.asRight
        case _                         => ().asLeft
      }
    } yield status
  }
}
