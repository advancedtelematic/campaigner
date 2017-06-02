package com.advancedtelematic.campaigner.db

import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import com.advancedtelematic.libats.slick.db.SlickExtensions._
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._
import scala.concurrent.{ExecutionContext, Future}
import slick.jdbc.MySQLProfile.api._

trait CampaignSupport {
  def Campaigns(implicit db: Database, ec: ExecutionContext) = new Campaigns()
}

protected class Campaigns()(implicit db: Database, ec: ExecutionContext) {

  import com.advancedtelematic.libats.slick.db.SlickAnyVal._

  def persist(campaign: Campaign, groups: Set[GroupId]): Future[Unit] =
    db.run {
      val f = for {
        _ <- Schema.campaigns += campaign
        _ <- Schema.campaignGroups ++= groups.map(g => (campaign.id, g))
      } yield ()

      f.transactionally
        .handleIntegrityErrors(Errors.ConflictingCampaign)
    }

  private[db] def find(ns: Namespace, campaign: CampaignId): DBIO[Campaign] =
    Schema.campaigns
      .filter(_.namespace === ns)
      .filter(_.id === campaign)
      .result
      .failIfNotSingle(Errors.CampaignMissing)

  def findCampaign(ns: Namespace, campaign: CampaignId): Future[Campaign] =
    db.run(find(ns, campaign))

  def findGroups(ns: Namespace, campaign: CampaignId): Future[Set[GroupId]] =
    db.run {
      find(ns, campaign).flatMap { _ =>
        Schema.campaignGroups
          .filter(_.campaignId === campaign)
          .map(_.groupId)
          .result
          .map(_.toSet)
      }
    }

  def update(ns: Namespace, campaign: CampaignId, name: String): Future[Unit] =
    db.run {
      find(ns, campaign).flatMap { _ =>
        Schema.campaigns
          .filter(_.id === campaign)
          .map(_.name)
          .update(name)
          .map(_ => ())
          .handleIntegrityErrors(Errors.ConflictingCampaign)
      }
    }

  def scheduleGroups(ns: Namespace, campaign: CampaignId, groups: Set[GroupId]): Future[Unit] =
    db.run {
      val f = for {
        _ <- find(ns, campaign)
        _ <- DBIO.sequence(groups.toSeq.map { group =>
          Schema.groupStats += GroupStats(campaign, group, GroupStatus.scheduled, 0, 0)
        })
      } yield ()

      f.transactionally
        .handleIntegrityErrors(Errors.CampaignAlreadyLaunched)
    }

  def aggregatedStatus(campaign: CampaignId): Future[CampaignStatus.Value] = {
    def groupStats(status: GroupStatus.Value) =
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

  def groupStatusFor(campaign: CampaignId, group: GroupId): Future[Option[GroupStatus.Value]] =
    db.run {
      Schema.groupStats
        .filter(_.campaignId === campaign)
        .filter(_.groupId === group)
        .map(_.status)
        .result
        .headOption
    }

  def failedDevices(ns: Namespace, campaign: CampaignId): Future[Set[DeviceId]] =
    db.run {
      find(ns, campaign).flatMap { _ =>
        Schema.deviceUpdates
          .filter(_.campaignId === campaign)
          .filter(_.status === DeviceStatus.failed)
          .map(_.deviceId)
          .result
          .map(_.toSet)
      }
    }

  def campaignStatsFor(ns: Namespace, campaign: CampaignId): Future[Map[GroupId, Stats]] =
    db.run {
      find(ns, campaign).flatMap { _ =>
        Schema.groupStats
          .filter(_.campaignId === campaign)
          .map(r => (r.groupId, r.processed, r.affected))
          .result
      }
    }.map(_.groupBy(_._1).map {
      case (group, (_, processed, affected) +: _) => (group, Stats(processed, affected))
    })

  def remainingCampaigns(): Future[Seq[Campaign]] =
    db.run {
      Schema.groupStats.join(Schema.campaigns).on(_.campaignId === _.id)
        .filter(_._1.status === GroupStatus.scheduled)
        .map(_._2)
        .result
    }

  def freshCampaigns(): Future[Seq[Campaign]] =
    db.run {
      Schema.groupStats.join(Schema.campaigns).on(_.campaignId === _.id)
        .filter(_._1.status === GroupStatus.scheduled)
        .filter(_._1.processed === 0L)
        .filter(_._1.affected  === 0L)
        .map(_._2)
        .result
    }

  def remainingGroups(campaign: CampaignId): Future[Seq[GroupId]] =
    db.run {
      Schema.groupStats
        .filter(_.campaignId === campaign)
        .filter(_.status === GroupStatus.scheduled)
        .map(_.groupId)
        .result
    }

  def remainingBatches(campaign: CampaignId, group: GroupId): Future[Option[Long]] =
    db.run {
      Schema.groupStats
        .filter(_.campaignId === campaign)
        .filter(_.groupId === group)
        .filter(_.status === GroupStatus.scheduled)
        .map(_.processed)
        .result
        .headOption
    }

  private[db] def progressGroup(
    ns: Namespace,
    campaign: CampaignId,
    group: GroupId,
    status: GroupStatus.Value,
    stats: Stats): Future[Unit] =
    db.run {
      if (stats.affected > stats.processed)
        DBIO.failed(Errors.InvalidCounts)
      else
        find(ns, campaign).flatMap { _ =>
          Schema.groupStats
            .filter(_.campaignId === campaign)
            .filter(_.groupId === group)
            .filter(s => s.affected <= s.processed)
            .map(s => (s.status, s.processed, s.affected))
            .update((status, stats.processed, stats.affected))
            .flatMap {
            case 0 => Schema.groupStats += GroupStats(campaign, group, status, stats.processed, stats.affected)
            case _ =>  DBIO.successful(())
          }
        }.map(_ => ()).transactionally
          .handleIntegrityErrors(Errors.CampaignMissing)
    }

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
    campaign: CampaignId) : Future[Unit] =
    db.run {
      find(ns, campaign).flatMap { _ =>
        Schema.groupStats
          .filter(_.campaignId === campaign)
          .map(_.status)
          .update(GroupStatus.cancelled)
          .map(_ => ())
      }
    }

  def scheduleDevice(campaign: CampaignId, update: UpdateId, device: DeviceId): Future[Unit] =
    db.run {
      (Schema.deviceUpdates += DeviceUpdate(campaign, update, device, DeviceStatus.scheduled))
        .map(_ => ())
    }

  def finishDevice(update: UpdateId, device: DeviceId, status: DeviceStatus.Value): Future[Unit] =
    db.run {
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

  def countFinished(ns: Namespace, campaignId: CampaignId): Future[Long] =
    db.run {
      Schema.campaigns
        .filter(_.namespace === ns)
        .filter(_.id === campaignId)
        .flatMap { campaign =>
        Schema.deviceUpdates
          .filter(_.campaignId === campaign.id)
          .filter(_.updateId === campaign.update)
          .filter(d => d.status === DeviceStatus.successful ||
                       d.status === DeviceStatus.failed)
          .map(_.deviceId)
      }.distinct
        .length
        .result
    }.map(_.toLong)

}
