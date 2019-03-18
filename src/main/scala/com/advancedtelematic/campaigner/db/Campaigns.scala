package com.advancedtelematic.campaigner.db

import cats.syntax.either._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType.DeviceStatus.DeviceStatus
import com.advancedtelematic.campaigner.data.DataType.SortBy.SortBy
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.SlickMapping._
import com.advancedtelematic.campaigner.http.Errors._
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

object Campaigns {
  def apply()(implicit db: Database, ec: ExecutionContext): Campaigns = new Campaigns()
}

protected [db] class Campaigns(implicit db: Database, ec: ExecutionContext)
    extends CampaignSupport
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

  /**
   * Given a campaign ID, returns IDs of all devices that are in `requested`
   * state
   */
  def requestedDevices(campaign: CampaignId): Future[Set[DeviceId]] =
    deviceUpdateRepo.findByCampaign(campaign, DeviceStatus.requested)

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

  /**
   * Returns total number of devices in the given campaigns
   */
  private def countProcessedAction(campaignIds: Set[CampaignId]): DBIO[Long] =
    campaignRepo.countDevices(campaignIds)(_ => true.bind)

  /**
   * Returns the number of affected devices in the given campaigns
   */
  private def countAffectedAction(campaignIds: Set[CampaignId]): DBIO[Long] =
    campaignRepo.countDevices(campaignIds)(status => status =!= DeviceStatus.rejected)

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
   * TODO this method needs separate refactoring
   */
  def campaignStats(campaignId: CampaignId): Future[CampaignStats] = db.run {
    val statsAction = for {
      mainCampaign <- campaignRepo.findAction(campaignId)
      retryCampaignIds <- campaignRepo.findRetryCampaignIdsOfAction(campaignId)
      mainProcessed <- countProcessedAction(Set(campaignId))
      mainAffected <- countAffectedAction(Set(campaignId))
      retryProcessed <- countProcessedAction(retryCampaignIds)
      retryAffected <- countAffectedAction(retryCampaignIds)
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

  def launch(campaignId: CampaignId): Future[Unit] = db.run {
    val action = for {
      campaign <- campaignRepo.findAction(campaignId)
      _ <- if (campaign.status != CampaignStatus.prepared) {
        throw CampaignAlreadyLaunched
      } else {
        DBIO.successful(())
      }
      _ <- campaignRepo.setStatusAction(campaignId, CampaignStatus.launched)
    } yield ()

    action.transactionally
  }

  def create(campaign: Campaign, groups: Set[GroupId], devices: Set[DeviceId], metadata: Seq[CampaignMetadata]): Future[CampaignId] =
    campaignRepo.persist(campaign, groups, devices, metadata)

  def update(id: CampaignId, name: String, metadata: Seq[CampaignMetadata]): Future[Unit] =
    campaignRepo.update(id, name, metadata)

  // TODO remove when FE is not dependent on this information anymore
  private def findGroupsAction(campaignId: CampaignId): DBIO[Set[GroupId]] =
    campaignRepo.findAction(campaignId).flatMap { _ =>
        Schema.campaignGroups
          .filter(_.campaignId === campaignId)
          .map(_.groupId)
          .result
          .map(_.toSet)
    }

  /**
   * Given a campaign and sets of accepted, scheduled and rejected
   * devices, calculates and updates campaign and devices statuses in the database.
   */
  def updateCampaignAndDevicesStatuses(
      campaign: Campaign,
      acceptedDeviceIds: Set[DeviceId],
      scheduledDeviceIds: Set[DeviceId],
      rejectedDeviceIds: Set[DeviceId]): Future[Unit] = {
    def persistDeviceUpdates(deviceIds: Set[DeviceId], status: DeviceStatus): DBIO[Unit] =
      deviceUpdateRepo.persistManyAction(deviceIds.toSeq.map(deviceId =>
        DeviceUpdate(campaign.id, campaign.updateId, deviceId, status)
      ))

    val action = for {
      _ <- persistDeviceUpdates(acceptedDeviceIds, DeviceStatus.accepted)
      _ <- persistDeviceUpdates(scheduledDeviceIds, DeviceStatus.scheduled)
      _ <- persistDeviceUpdates(rejectedDeviceIds, DeviceStatus.rejected)
      _ <- campaignStatusTransition.updateToCalculatedStatus(campaign.id)
    } yield ()

    db.run(action.transactionally)
  }
}

// TODO refactor and get rid of this class
protected [db] class CampaignStatusTransition(implicit db: Database, ec: ExecutionContext)
    extends CampaignSupport
    with CancelTaskSupport  {

  def devicesFinished(campaignId: CampaignId): DBIO[Unit] =
    updateToCalculatedStatus(campaignId)

  def cancel(campaignId: CampaignId): DBIO[Unit] =
    for {
      _ <- campaignRepo.findAction(campaignId)
      _ <- cancelTaskRepo.cancelAction(campaignId)
      _ <- campaignRepo.setStatusAction(campaignId, CampaignStatus.cancelled)
    } yield ()

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
