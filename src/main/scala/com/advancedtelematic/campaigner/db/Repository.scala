package com.advancedtelematic.campaigner.db

import com.advancedtelematic.campaigner.data.DataType.CampaignStatus._
import com.advancedtelematic.campaigner.data.DataType.DeviceStatus._
import com.advancedtelematic.campaigner.data.DataType.GroupStatus._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Schema.{GroupStatsTable}
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import com.advancedtelematic.libats.slick.db.SlickExtensions._
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._

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

protected [db] class DeviceUpdateRepository()(implicit db: Database, ec: ExecutionContext) {

  protected [db] def findByCampaignAction(campaign: CampaignId, status: DeviceStatus): DBIO[Set[DeviceId]] =
    Schema.deviceUpdates
      .filter(_.campaignId === campaign)
      .filter(_.status === status)
      .map(_.deviceId)
      .result
      .map(_.toSet)

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

    db.run {
      for {
        scheduled <- groupStats(GroupStatus.scheduled)
        launched  <- groupStats(GroupStatus.launched)
        cancelled <- groupStats(GroupStatus.cancelled)
        status     = (scheduled, launched, cancelled) match {
          case (_, _, c) if c > 0 => CampaignStatus.cancelled
          case (0, l, _) if l > 0 => CampaignStatus.launched
          case (0, 0, _)          => CampaignStatus.prepared
          case _                  => CampaignStatus.scheduled
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

  def persist(campaign: Campaign, groups: Set[GroupId]): Future[CampaignId] =
    db.run {
      val f = for {
        _ <- Schema.campaigns += campaign
        _ <- Schema.campaignGroups ++= groups.map(g => (campaign.id, g))
      } yield campaign.id

      f.transactionally.handleIntegrityErrors(Errors.ConflictingCampaign)
    }

  def find(ns: Namespace, campaign: CampaignId): Future[Campaign] =
    db.run(findAction(ns, campaign))

  protected[db] def findAction(ns: Namespace, campaign: CampaignId): DBIO[Campaign] =
    Schema.campaigns
      .filter(_.namespace === ns)
      .filter(_.id === campaign)
      .result
      .failIfNotSingle(Errors.CampaignMissing)

  def findCampaign(ns: Namespace, campaign: CampaignId): Future[Campaign] =
    db.run(findAction(ns, campaign))

  def findAllScheduled(filter: GroupStatsTable => Rep[Boolean] = _ => true.bind): Future[Seq[Campaign]] = {
    db.run {
      Schema.groupStats.join(Schema.campaigns).on(_.campaignId === _.id)
        .filter { case (groupStats, _) => groupStats.status === GroupStatus.scheduled }
        .filter { case (groupStats, _) => filter(groupStats) }
        .map(_._2)
        .result
    }
  }

  def updateName(ns: Namespace, campaign: CampaignId, name: String): Future[Unit] =
    db.run {
      findAction(ns, campaign).flatMap { _ =>
        Schema.campaigns
          .filter(_.id === campaign)
          .map(_.name)
          .update(name)
          .map(_ => ())
          .handleIntegrityErrors(Errors.ConflictingCampaign)
      }
    }

  def countFinished(ns: Namespace, campaign: CampaignId): Future[Long] = db.run {
    Schema.campaigns
      .filter(_.namespace === ns)
      .filter(_.id === campaign)
      .join(Schema.deviceUpdates)
      .on { case (campaign, update) => campaign.update === update.updateId && campaign.id === update.campaignId}
      .filter { case (_, update) => update.status === DeviceStatus.successful || update.status === DeviceStatus.failed }
      .distinct
      .length
      .result
      .map(_.toLong)
  }
}
