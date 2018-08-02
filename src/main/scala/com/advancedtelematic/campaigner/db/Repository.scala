package com.advancedtelematic.campaigner.db

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.advancedtelematic.campaigner.data.DataType.CancelTaskStatus.CancelTaskStatus
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus._
import com.advancedtelematic.campaigner.data.DataType.DeviceStatus._
import com.advancedtelematic.campaigner.data.DataType.GroupStatus._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Schema.{CampaignGroupsTable, GroupStatsTable}
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import com.advancedtelematic.libats.slick.db.SlickAnyVal._
import com.advancedtelematic.libats.slick.db.SlickExtensions._
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._
import SlickMapping._
import akka.http.scaladsl.util.FastFuture

import scala.concurrent.{ExecutionContext, Future}
import slick.jdbc.MySQLProfile.api._

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

trait CampaignMetadataSupport {
  def campaignMetadataRepo(implicit db: Database, ec: ExecutionContext) = new CampaignMetadataRepository()
}

protected [db] class CampaignMetadataRepository()(implicit db: Database, ec: ExecutionContext) {
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

  protected [db] def findByCampaignAction(campaign: CampaignId, status: DeviceStatus): DBIO[Set[DeviceId]] =
    Schema.deviceUpdates
      .filter(_.campaignId === campaign)
      .filter(_.status === status)
      .map(_.deviceId)
      .result
      .map(_.toSet)

  def findByCampaignStream(campaign: CampaignId, status: DeviceStatus): Source[DeviceId, NotUsed] =
    Source.fromPublisher {
      db.stream {
        Schema.deviceUpdates
          .filter(_.campaignId === campaign)
          .filter(_.status === status)
          .map(_.deviceId)
          .result
      }
    }

  def setUpdateStatus(update: UpdateId, device: DeviceId, status: DeviceStatus): Future[Unit] = db.run {
    Schema.deviceUpdates
      .filter(_.updateId === update)
      .filter(_.deviceId === device)
      .map(_.status)
      .update(status)
      .flatMap {
        case 0 => DBIO.failed(Errors.DeviceNotScheduled)
        case _ => DBIO.successful(())
      }.map(_ => ())
  }

  def setUpdateStatus(campaign: CampaignId, devices: Seq[DeviceId], status: DeviceStatus): Future[Unit] = db.run {
    Schema.deviceUpdates
      .filter(_.campaignId === campaign)
      .filter(_.deviceId inSet devices)
      .map(_.status)
      .update(status)
      .flatMap {
        case n if devices.length == n => DBIO.successful(())
        case _ => DBIO.failed(Errors.DeviceNotScheduled)
      }.map(_ => ())
  }

  def persist(update: DeviceUpdate): Future[Unit] = db.run {
    (Schema.deviceUpdates += update).map(_ => ())
  }
}

protected [db] class GroupStatsRepository()(implicit db: Database, ec: ExecutionContext) {
  def updateGroupStats(campaign: CampaignId, group: GroupId, status: GroupStatus, stats: Stats): Future[Unit] =
    db.run {
      Schema.groupStats
        .insertOrUpdate(GroupStats(campaign, group, status, stats.processed, stats.affected))
        .map(_ => ())
        .handleIntegrityErrors(Errors.CampaignMissing)
    }

  protected [db] def findByCampaignAction(campaign: CampaignId): DBIO[Seq[GroupStats]] =
    Schema.groupStats
      .filter(_.campaignId === campaign)
      .result

  protected [db] def persistManyAction(campaign: CampaignId, groups: Set[GroupId]): DBIO[Unit] =
    DBIO.sequence {
      groups.toSeq.map { group =>
        persistAction(GroupStats(campaign, group, GroupStatus.scheduled, 0, 0))
      }
    }.map(_ => ())

  protected [db] def persistAction(stats: GroupStats): DBIO[Unit] = (Schema.groupStats += stats).map(_ => ())

  def aggregatedStatus(campaign: CampaignId): Future[CampaignStatus] = {
    def groupStats(status: GroupStatus) =
      Schema.groupStats
        .filter(_.campaignId === campaign)
        .filter(_.status === status)
        .length.result

    def affectedDevices() =
      Schema.groupStats
        .filter(_.campaignId === campaign)
        .map(_.affected)
        .sum.result.map(_.getOrElse(0L))

    def finishedDevices() =
      Schema.deviceUpdates
        .filter(_.campaignId === campaign)
        .map(_.status)
        .filter(status => status === DeviceStatus.successful || status === DeviceStatus.failed)
        .length.result

    db.run {
      for {
        // groups
        groups    <- findByCampaignAction(campaign).map(_.length)
        scheduled <- groupStats(GroupStatus.scheduled)
        launched  <- groupStats(GroupStatus.launched)
        cancelled <- groupStats(GroupStatus.cancelled)

        // devices
        affected <- affectedDevices()
        finished <- finishedDevices()
        status    = (groups, scheduled, launched, cancelled, affected, finished) match {
          case (_, _, _, c, _, _) if c > 0           => CampaignStatus.cancelled
          case (g, 0, _, _, a, f) if g > 0 && a == f => CampaignStatus.finished
          case (0, 0, 0, _, _, _)                    => CampaignStatus.prepared
          case (_, 0, _, _, _, _)                    => CampaignStatus.launched
          case _                                     => CampaignStatus.scheduled
        }
      } yield status
    }
  }

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

  def cancel(campaign: CampaignId): Future[Unit] = db.run {
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
      _ <- (Schema.campaigns += campaign).handleIntegrityErrors(Errors.ConflictingCampaign)
      _ <- Schema.campaignGroups ++= groups.map(g => (campaign.id, g))
      _ <- (Schema.campaignMetadata ++= metadata).handleIntegrityErrors(Errors.ConflictingMetadata)
    } yield campaign.id

    f.transactionally
  }

  protected[db] def findAction(ns: Namespace, campaign: CampaignId): DBIO[Campaign] =
    Schema.campaigns
      .filter(_.namespace === ns)
      .filter(_.id === campaign)
      .result
      .failIfNotSingle(Errors.CampaignMissing)

  protected[db] def findByUpdateAction(ns: Namespace, update: UpdateId): DBIO[Seq[CampaignId]] =
    Schema.campaigns
      .filter(_.namespace === ns)
      .filter(_.update === update)
      .map(_.id)
      .result

  def find(ns: Namespace, campaign: CampaignId): Future[Campaign] =
    db.run(findAction(ns, campaign))

  def all(ns: Namespace, offset: Long, limit: Long): Future[PaginationResult[CampaignId]] =
    db.run {
      Schema.campaigns
        .filter(_.namespace === ns)
        .map(_.id)
        .paginateResult(offset = offset, limit = limit)
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

  def update(ns: Namespace, campaign: CampaignId, name: String, metadata: Seq[CampaignMetadata]): Future[Unit] =
    db.run {
      findAction(ns, campaign).flatMap { _ =>
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

  def countDevices(ns: Namespace, campaign: CampaignId)(filterExpr: Rep[DeviceStatus] => Rep[Boolean]): Future[Long] = db.run {
    Schema.campaigns
      .filter(_.namespace === ns)
      .filter(_.id === campaign)
      .join(Schema.deviceUpdates)
      .on { case (campaign, update) => campaign.update === update.updateId && campaign.id === update.campaignId }
      .filter { case (_, update) => filterExpr(update.status) }
      .distinct
      .length
      .result
      .map(_.toLong)
  }

}


protected class CancelTaskRepository()(implicit db: Database, ec: ExecutionContext) {
  def cancel(campaign: CampaignId): Future[CancelTask] = db.run {
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
      .join(Schema.campaigns).on{case (task, campaign) => task.campaignId === campaign.id}
      .map(_._2)
      .map (c => (c.namespace, c.id))
      .distinct
      .result

  def findPending(): Future[Seq[(Namespace, CampaignId)]] = db.run {
    findStatus(CancelTaskStatus.pending)
  }

  def findInprogress(): Future[Seq[(Namespace, CampaignId)]] = db.run {
    findStatus(CancelTaskStatus.inprogress)
  }
}
