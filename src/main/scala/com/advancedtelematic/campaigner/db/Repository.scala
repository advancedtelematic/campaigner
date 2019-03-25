package com.advancedtelematic.campaigner.db

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.data.NonEmptyList
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus._
import com.advancedtelematic.campaigner.data.DataType.CancelTaskStatus.CancelTaskStatus
import com.advancedtelematic.campaigner.data.DataType.DeviceStatus._
import com.advancedtelematic.campaigner.data.DataType.SortBy.SortBy
import com.advancedtelematic.campaigner.data.DataType.UpdateType.UpdateType
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Errors.{CampaignFKViolation, UpdateFKViolation}
import com.advancedtelematic.campaigner.db.SlickMapping._
import com.advancedtelematic.campaigner.db.SlickUtil.{sortBySlickOrderedCampaignConversion, sortBySlickOrderedUpdateConversion}
import com.advancedtelematic.campaigner.http.Errors._
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import com.advancedtelematic.libats.slick.db.SlickAnyVal._
import com.advancedtelematic.libats.slick.db.SlickExtensions._
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._
import java.util.UUID
import slick.jdbc.MySQLProfile.api._
import slick.jdbc.GetResult

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

trait CampaignSupport {
  def campaignRepo(implicit db: Database, ec: ExecutionContext) = new CampaignRepository()
}

trait DeviceUpdateSupport {
  def deviceUpdateRepo(implicit db: Database, ec: ExecutionContext) = new DeviceUpdateRepository()
}

trait CancelTaskSupport {
  def cancelTaskRepo(implicit db: Database, ec: ExecutionContext) = new CancelTaskRepository()
}

trait UpdateSupport {
  def updateRepo(implicit db: Database, ec: ExecutionContext) = new UpdateRepository()
}

trait CampaignMetadataSupport {
  def campaignMetadataRepo(implicit db: Database) = new CampaignMetadataRepository()
}

protected [db] class CampaignMetadataRepository()(implicit db: Database) {
  def findFor(campaign: CampaignId): Future[Seq[CampaignMetadata]] = db.run {
    Schema.campaignMetadata.filter(_.campaignId === campaign).result
  }
}


protected [db] class DeviceUpdateRepository()(implicit db: Database, ec: ExecutionContext) {

  def findDeviceCampaigns(deviceId: DeviceId, status: DeviceStatus*): Future[Seq[(Campaign, Option[CampaignMetadata])]] = db.run {
    assert(status.nonEmpty)

    Schema.deviceUpdates
      .filter { d => d.deviceId === deviceId && d.status.inSet(status) }
      .join(Schema.campaigns).on(_.campaignId === _.id)
      .joinLeft(Schema.campaignMetadata).on { case ((d, c), m) => c.id === m.campaignId }
      .map { case ((d, c), m) => (c, m) }
      .result
  }

  def findByCampaign(campaign: CampaignId, status: DeviceStatus): Future[Set[DeviceId]] =
    db.run(findByCampaignAction(campaign, status))

  protected [db] def findByCampaignAction(campaign: CampaignId, status: DeviceStatus): DBIO[Set[DeviceId]] =
    Schema.deviceUpdates
      .filter(_.campaignId === campaign)
      .filter(_.status === status)
      .map(_.deviceId)
      .result
      .map(_.toSet)

  def findByCampaignStream(campaign: CampaignId, status: DeviceStatus*): Source[DeviceId, NotUsed] =
    Source.fromPublisher {
      db.stream {
        Schema.deviceUpdates
          .filter(_.campaignId === campaign)
          .filter(_.status.inSet(status))
          .map(_.deviceId)
          .result
      }
    }

  /**
    * Find all the device updates that failed with a given failure code.
    */
  def findFailedByFailureCode(campaignId: CampaignId, failureCode: String): Future[Set[DeviceId]] = db.run {
    Schema.deviceUpdates
      .filter(_.campaignId === campaignId)
      .filter(_.status === DeviceStatus.failed)
      .filter(_.resultCode === failureCode)
      .map(_.deviceId)
      .result
      .map(_.toSet)
  }

  protected [db] def setUpdateStatusAction(update: UpdateId, device: DeviceId, status: DeviceStatus, resultCode: Option[String]): DBIO[Unit] =
    Schema.deviceUpdates
      .filter(_.updateId === update)
      .filter(_.deviceId === device)
      .map(du => (du.status, du.resultCode))
      .update((status, resultCode))
      .flatMap {
        case 0 => DBIO.failed(DeviceNotScheduled)
        case _ => DBIO.successful(())
      }.map(_ => ())

  protected [db] def setUpdateStatusAction(campaign: CampaignId, devices: Seq[DeviceId], status: DeviceStatus, resultCode: Option[String]): DBIO[Unit] =
    Schema.deviceUpdates
      .filter(_.campaignId === campaign)
      .filter(_.deviceId inSet devices)
      .map(du => (du.status, du.resultCode))
      .update((status, resultCode))
      .flatMap {
        case n if devices.length == n => DBIO.successful(())
        case _ => DBIO.failed(DeviceNotScheduled)
      }.map(_ => ())

  def persistMany(updates: Seq[DeviceUpdate]): Future[Unit] =
    db.run(persistManyAction(updates))

  def persistManyAction(updates: Seq[DeviceUpdate]): DBIO[Unit] =
    DBIO.sequence(updates.map(Schema.deviceUpdates.insertOrUpdate)).transactionally.map(_ => ())

  /**
   * Given a set of campaigns, finds device updates happend in them, groups them
   * by status and counts.
   */
  def countByStatus(campaignIds: Set[CampaignId]): DBIO[Map[DeviceStatus, Int]] = {
    Schema.deviceUpdates
      .filter(_.campaignId.inSet(campaignIds))
      .groupBy(_.status)
      .map { case (st, upds) => (st, upds.size) }
      .result
      .map(_.toMap)
  }
}

protected class CampaignRepository()(implicit db: Database, ec: ExecutionContext) {
  import com.advancedtelematic.libats.slick.db.SlickAnyVal._

  def persist(campaign: Campaign, groups: Set[GroupId], devices: Set[DeviceId], metadata: Seq[CampaignMetadata]): Future[CampaignId] = db.run {
    val f = for {
      _ <- (Schema.campaigns += campaign).recover {
        case Failure(UpdateFKViolation()) => DBIO.failed(MissingUpdateSource)
        case Failure(CampaignFKViolation()) => DBIO.failed(MissingMainCampaign)
      }.handleIntegrityErrors(ConflictingCampaign)
      _ <- Schema.campaignGroups ++= groups.map(campaign.id -> _)
      _ <- Schema.deviceUpdates ++= devices.map(did => DeviceUpdate(campaign.id, campaign.updateId, did, DeviceStatus.requested))
      _ <- (Schema.campaignMetadata ++= metadata).handleIntegrityErrors(ConflictingMetadata)
    } yield campaign.id

    f.transactionally
  }

  protected[db] def findAction(campaign: CampaignId, ns: Option[Namespace] = None): DBIO[Campaign] =
    Schema.campaigns
      .maybeFilter(_.namespace === ns)
      .filter(_.id === campaign)
      .result
      .failIfNotSingle(CampaignMissing)

  protected[db] def findByUpdateAction(update: UpdateId): DBIO[Seq[Campaign]] =
    Schema.campaigns.filter(_.update === update).result

  def find(campaign: CampaignId, ns: Option[Namespace] = None): Future[Campaign] =
    db.run(findAction(campaign, ns))

  def all(ns: Namespace, sortBy: SortBy, offset: Long, limit: Long, status: Option[CampaignStatus], nameContains: Option[String]): Future[PaginationResult[CampaignId]] = {
    db.run {
      Schema.campaigns
        .filter(_.namespace === ns)
        .filter(_.mainCampaignId.isEmpty)
        .maybeFilter(_.status === status)
        .maybeContains(_.name, nameContains)
        .sortBy(sortBy)
        .map(_.id)
        .paginateResult(offset, limit)
    }
  }

  /**
   * Returns all campaigns that have at least one device in `requested` state
   */
  def findAllWithRequestedDevices: DBIO[Set[Campaign]] =
    Schema.deviceUpdates.join(Schema.campaigns).on(_.campaignId === _.id)
      .filter { case (deviceUpdate, _) => deviceUpdate.status === DeviceStatus.requested }
      .map(_._2)
      .result
      .map(_.toSet)

  /**
   * Returns all campaigns that have all devices in `requested` state
   */
  def findAllNewlyCreated: DBIO[Set[Campaign]] = {
    implicit val `GetResult[UUID]` = GetResult(r => UUID.fromString(r.nextString))

    val queryAllNewlyCreatedCampaignIds = sql"""
      select campaign_id
      from device_updates
      group by campaign_id
      having group_concat(distinct status) = 'requested';
    """.as[UUID]

    val action = for {
      ids <- queryAllNewlyCreatedCampaignIds.map(_.map(CampaignId(_)))
      campaigns <- Schema.campaigns.filter(_.id.inSet(ids)).result
    } yield campaigns.toSet

    action.transactionally
  }

  def update(campaign: CampaignId, name: String, metadata: Seq[CampaignMetadata]): Future[Unit] =
    db.run {
      findAction(campaign).flatMap { _ =>
        Schema.campaigns
          .filter(_.id === campaign)
          .map(_.name)
          .update(name)
          .handleIntegrityErrors(ConflictingCampaign)
      }.andThen {
        Schema.campaignMetadata.filter(_.campaignId === campaign).delete
      }.andThen {
        (Schema.campaignMetadata ++= metadata).handleIntegrityErrors(ConflictingMetadata)
      }.map(_ => ())
    }

  def countByStatus: DBIO[Map[CampaignStatus, Int]] =
    Schema.campaigns
    .groupBy(_.status)
    .map { case (status, campaigns) => status -> campaigns.length }
    .result
    .map(_.toMap)

  /**
   * Returns the number of devices that took part in the given campaigns and
   * matched the filter expression.
   */
  protected[db] def countDevices(campaignIds: Set[CampaignId])(filterExpr: Rep[DeviceStatus] => Rep[Boolean]): DBIO[Long] =
    Schema.campaigns
      .filter(_.id.inSet(campaignIds))
      .join(Schema.deviceUpdates)
      .on { case (c, u) => c.update === u.updateId && c.id === u.campaignId }
      .filter { case (_, update) => filterExpr(update.status) }
      .distinct
      .length
      .result
      .map(_.toLong)

  def setStatusAction(campaignId: CampaignId, status: CampaignStatus): DBIO[CampaignId] =
    Schema.campaigns
      .filter(_.id === campaignId).map(_.status).update(status).map(_ => campaignId)

  /**
   * Given a main campaign ID, finds all IDs of the corresponding retry
   * campaigns.
   */
  def findRetryCampaignIdsOf(mainId: CampaignId): Future[Set[CampaignId]] =
    db.run(findRetryCampaignIdsOfAction(mainId))

  /**
   * Given a main campaign ID, finds all IDs of the corresponding retry
   * campaigns.
   */
  protected[db] def findRetryCampaignIdsOfAction(mainId: CampaignId): DBIO[Set[CampaignId]] =
    Schema.campaigns
      .filter(_.mainCampaignId === mainId)
      .map(_.id)
      .result
      .map(_.toSet)
}


protected class CancelTaskRepository()(implicit db: Database, ec: ExecutionContext) {
  protected [db] def cancelAction(campaign: CampaignId): DBIO[CancelTask] = {
    val cancel = CancelTask(campaign, CancelTaskStatus.pending)
    (Schema.cancelTasks += cancel)
      .map(_ => cancel)
  }

  def setStatus(campaign: CampaignId, status: CancelTaskStatus): Future[Unit] = db.run {
    Schema.cancelTasks
      .filter(_.campaignId === campaign)
      .map(_.taskStatus)
      .update(status)
      .map(_ => ())
  }

  private def findStatus(status: CancelTaskStatus): DBIO[Seq[(Namespace, CampaignId)]] =
    Schema.cancelTasks
      .filter(_.taskStatus === status)
      .join(Schema.campaigns).on { case (task, campaign) => task.campaignId === campaign.id }
      .map(_._2)
      .map(c => (c.namespace, c.id))
      .distinct
      .result

  def findPending(): Future[Seq[(Namespace, CampaignId)]] = db.run {
    findStatus(CancelTaskStatus.pending)
  }

  def findInprogress(): Future[Seq[(Namespace, CampaignId)]] = db.run {
    findStatus(CancelTaskStatus.inprogress)
  }
}

protected class UpdateRepository()(implicit db: Database, ec: ExecutionContext) {
  import com.advancedtelematic.libats.slick.db.SlickPagination._

  def persist(update: Update): Future[UpdateId] = db.run {
    (Schema.updates += update).map(_ => update.uuid).handleIntegrityErrors(ConflictingUpdate)
  }

  def findById(id: UpdateId): Future[Update] = db.run {
    Schema.updates.filter(_.uuid === id).result.failIfNotSingle(MissingUpdate(id))
  }

  def findByIds(ns: Namespace, ids: NonEmptyList[UpdateId]): Future[List[Update]] = db.run(
    Schema.updates.filter(xs => xs.namespace === ns && xs.uuid.inSet(ids.toList)).to[List].result
  )

  private def findByExternalIdsAction(ns: Namespace, ids: Seq[ExternalUpdateId]): DBIO[Seq[Update]] =
    Schema.updates.filter(_.namespace === ns).filter(_.updateId.inSet(ids)).result

  def findByExternalIds(ns: Namespace, ids: Seq[ExternalUpdateId]): Future[Seq[Update]] = db.run {
    findByExternalIdsAction(ns, ids)
  }

  def findByExternalId(ns: Namespace, id: ExternalUpdateId): Future[Update] = db.run {
    findByExternalIdsAction(ns, Seq(id)).failIfNotSingle(MissingExternalUpdate(id))
  }

  def all(ns: Namespace, sortBy: SortBy = SortBy.Name, nameContains: Option[String] = None, updateType: Option[UpdateType] = None): Future[Seq[Update]] = db.run {
    Schema.updates
      .filter(_.namespace === ns)
      .maybeFilter(_.updateSourceType === updateType)
      .maybeContains(_.name, nameContains)
      .sortBy(sortBy)
      .result
  }

  def allPaginated(ns: Namespace, sortBy: SortBy, offset: Long, limit: Long, nameContains: Option[String]): Future[PaginationResult[Update]] = db.run {
    Schema.updates
      .filter(_.namespace === ns)
      .maybeContains(_.name, nameContains)
      .paginateAndSortResult(sortBy, offset, limit)
  }
}
