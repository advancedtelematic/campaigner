package com.advancedtelematic.campaigner.db

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.data.NonEmptyList
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus._
import com.advancedtelematic.campaigner.data.DataType.CancelTaskStatus.CancelTaskStatus
import com.advancedtelematic.campaigner.data.DataType.DeviceStatus._
import com.advancedtelematic.campaigner.data.DataType.GroupStatus._
import com.advancedtelematic.campaigner.data.DataType.SortBy.SortBy
import com.advancedtelematic.campaigner.data.DataType.UpdateType.UpdateType
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.DBErrors.{CampaignCampaignFKViolation, CampaignUpdateFKViolation}
import com.advancedtelematic.campaigner.db.Schema.GroupStatsTable
import com.advancedtelematic.campaigner.db.SlickMapping._
import com.advancedtelematic.campaigner.db.SlickUtil.{sortBySlickOrderedCampaignConversion, sortBySlickOrderedUpdateConversion}
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import com.advancedtelematic.libats.slick.db.SlickAnyVal._
import com.advancedtelematic.libats.slick.db.SlickExtensions._
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

trait CampaignSupport {
  def campaignRepo(implicit db: Database, ec: ExecutionContext) = new CampaignRepository()
}

trait GroupStatsSupport {
  def groupStatsRepo(implicit db: Database, ec: ExecutionContext) = new GroupStatsRepository()
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

  protected [db] def setUpdateStatusAction(update: UpdateId, device: DeviceId, status: DeviceStatus): DBIO[Unit] =
    Schema.deviceUpdates
      .filter(_.updateId === update)
      .filter(_.deviceId === device)
      .map(_.status)
      .update(status)
      .flatMap {
        case 0 => DBIO.failed(Errors.DeviceNotScheduled)
        case _ => DBIO.successful(())
      }.map(_ => ())

  protected [db] def setUpdateStatusAction(campaign: CampaignId, devices: Seq[DeviceId], status: DeviceStatus): DBIO[Unit] =
    Schema.deviceUpdates
      .filter(_.campaignId === campaign)
      .filter(_.deviceId inSet devices)
      .map(_.status)
      .update(status)
      .flatMap {
        case n if devices.length == n => DBIO.successful(())
        case _ => DBIO.failed(Errors.DeviceNotScheduled)
      }.map(_ => ())

  def persistMany(updates: Seq[DeviceUpdate]): Future[Unit] = db.run {
    DBIO.sequence(updates.map(Schema.deviceUpdates.insertOrUpdate)).transactionally.map(_ => ())
  }
}

protected [db] class GroupStatsRepository()(implicit db: Database, ec: ExecutionContext) {
  protected [db] def updateGroupStatsAction(campaign: CampaignId, group: GroupId, status: GroupStatus, stats: Stats): DBIO[Unit] =
    Schema.groupStats
      .insertOrUpdate(GroupStats(campaign, group, status, stats.processed, stats.affected))
      .map(_ => ())
      .handleIntegrityErrors(Errors.CampaignMissing)

  protected [db] def findByCampaignAction(campaign: CampaignId): DBIO[Seq[GroupStats]] =
    Schema.groupStats
      .filter(_.campaignId === campaign)
      .result

  protected [db] def persistManyAction(campaign: CampaignId, groups: Set[GroupId]): DBIO[Unit] =
    DBIO.sequence(
      groups.toSeq.map { group =>
        persistAction(GroupStats(campaign, group, GroupStatus.scheduled, 0, 0))
      }
    ).map(_ => ())

  protected [db] def persistAction(stats: GroupStats): DBIO[Unit] = (Schema.groupStats += stats).map(_ => ())

  def groupStatusFor(campaign: CampaignId, group: GroupId): Future[Option[GroupStatus]] =
    db.run {
      Schema.groupStats
        .filter(_.campaignId === campaign)
        .filter(_.groupId === group)
        .map(_.status)
        .result
        .headOption
    }

  def findScheduled(campaign: CampaignId, groupId: Option[GroupId] = None): Future[Seq[GroupStats]] = db.run {
    Schema.groupStats
      .filter(_.campaignId === campaign)
      .filter(_.status === GroupStatus.scheduled)
      .maybeFilter(_.groupId === groupId)
      .result
  }

  protected [db] def cancelAction(campaign: CampaignId): DBIO[Unit] = {
    Schema.groupStats
      .filter(_.campaignId === campaign)
      .map(_.status)
      .update(GroupStatus.cancelled)
      .map(_ => ())
  }
}

protected class CampaignRepository()(implicit db: Database, ec: ExecutionContext) {
  import com.advancedtelematic.libats.slick.db.SlickAnyVal._

  def persist(campaign: Campaign, groups: Set[GroupId], metadata: Seq[CampaignMetadata]): Future[CampaignId] = db.run {
    val f = for {
      _ <- (Schema.campaigns += campaign).recover {
        case Failure(CampaignUpdateFKViolation()) => DBIO.failed(Errors.MissingUpdateSource)
        case Failure(CampaignCampaignFKViolation()) => DBIO.failed(Errors.MissingParentCampaign)
      }.handleIntegrityErrors(Errors.ConflictingCampaign)
      _ <- Schema.campaignGroups ++= groups.map(campaign.id -> _)
      _ <- (Schema.campaignMetadata ++= metadata).handleIntegrityErrors(Errors.ConflictingMetadata)
    } yield campaign.id

    f.transactionally
  }

  protected[db] def findAction(campaign: CampaignId, ns: Option[Namespace] = None): DBIO[Campaign] =
    Schema.campaigns
      .maybeFilter(_.namespace === ns)
      .filter(_.id === campaign)
      .result
      .failIfNotSingle(Errors.CampaignMissing)

  protected[db] def findByUpdateAction(update: UpdateId): DBIO[Seq[Campaign]] =
    Schema.campaigns.filter(_.update === update).result

  def find(campaign: CampaignId, ns: Option[Namespace] = None): Future[Campaign] =
    db.run(findAction(campaign, ns))

  def all(ns: Namespace, sortBy: SortBy, offset: Long, limit: Long, status: Option[CampaignStatus], nameContains: Option[String]): Future[PaginationResult[CampaignId]] = {
    db.run {
      Schema.campaigns
        .filter(_.namespace === ns)
        .filter(_.parentCampaignId.isEmpty)
        .maybeFilter(_.status === status)
        .maybeContains(_.name, nameContains)
        .sortBy(sortBy)
        .map(_.id)
        .paginateResult(offset, limit)
    }
  }

  def findAllScheduled(filter: GroupStatsTable => Rep[Boolean] = _ => true.bind): Future[Seq[Campaign]] = {
    db.run {
      Schema.groupStats.join(Schema.campaigns).on(_.campaignId === _.id)
        .filter { case (groupStats, _) => groupStats.status === GroupStatus.scheduled }
        .filter { case (groupStats, _) => filter(groupStats) }
        .map(_._2)
        .result
    }
  }

  def update(campaign: CampaignId, name: String, metadata: Seq[CampaignMetadata]): Future[Unit] =
    db.run {
      findAction(campaign).flatMap { _ =>
        Schema.campaigns
          .filter(_.id === campaign)
          .map(_.name)
          .update(name)
          .handleIntegrityErrors(Errors.ConflictingCampaign)
      }.andThen {
        Schema.campaignMetadata.filter(_.campaignId === campaign).delete
      }.andThen {
        (Schema.campaignMetadata ++= metadata).handleIntegrityErrors(Errors.ConflictingMetadata)
      }.map(_ => ())
    }

  def countByStatus: DBIO[Map[CampaignStatus, Int]] =
    Schema.campaigns
    .groupBy(_.status)
    .map { case (status, campaigns) => status -> campaigns.length }
    .result
    .map(_.toMap)

  def countDevices(campaign: CampaignId)(filterExpr: Rep[DeviceStatus] => Rep[Boolean]): Future[Long] = db.run {
    Schema.campaigns
      .filter(_.id === campaign)
      .join(Schema.deviceUpdates)
      .on { case (c, u) => c.update === u.updateId && c.id === u.campaignId }
      .filter { case (_, update) => filterExpr(update.status) }
      .distinct
      .length
      .result
      .map(_.toLong)
  }

  def setStatusAction(campaignId: CampaignId, status: CampaignStatus): DBIO[CampaignId] =
    Schema.campaigns
      .filter(_.id === campaignId).map(_.status).update(status).map(_ => campaignId)
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
    (Schema.updates += update).map(_ => update.uuid).handleIntegrityErrors(Errors.ConflictingUpdate)
  }

  def findById(id: UpdateId): Future[Update] = db.run {
    Schema.updates.filter(_.uuid === id).result.failIfNotSingle(Errors.MissingUpdate(id))
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
    findByExternalIdsAction(ns, Seq(id)).failIfNotSingle(Errors.MissingExternalUpdate(id))
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
