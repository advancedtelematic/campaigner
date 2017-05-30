package com.advancedtelematic.campaigner.db

import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libats.data.Namespace
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
      for {
        _ <- find(ns, campaign)
        _ <- DBIO.sequence(groups.toSeq.map { group =>
          Schema.campaignStats += CampaignStats(campaign, group, false, 0, 0)
        })
      } yield ()
    }

  def campaignStatsFor(ns: Namespace, campaign: CampaignId): Future[Map[GroupId, Stats]] =
    db.run {
      find(ns, campaign).flatMap { _ =>
        Schema.campaignStats
          .filter(_.campaignId === campaign)
          .map(r => (r.groupId, r.processed, r.affected))
          .result
      }
    }.map(_.groupBy(_._1).map {
      case (group, (_, processed, affected) +: _) => (group, Stats(processed, affected))
    })

  def remainingCampaigns(): Future[Seq[Campaign]] =
    db.run {
      Schema.campaignStats.join(Schema.campaigns).on(_.campaignId === _.id)
        .filter(!_._1.completed)
        .map(_._2)
        .result
    }

  def freshCampaigns(): Future[Seq[Campaign]] =
    db.run {
      Schema.campaignStats.join(Schema.campaigns).on(_.campaignId === _.id)
        .filter(!_._1.completed)
        .filter(_._1.processed === 0L)
        .filter(_._1.affected  === 0L)
        .map(_._2)
        .result
    }

  def remainingGroups(campaign: CampaignId): Future[Seq[GroupId]] =
    db.run {
      Schema.campaignStats
        .filter(_.campaignId === campaign)
        .filter(!_.completed)
        .map(_.groupId)
        .result
    }

  def remainingBatches(campaign: CampaignId, group: GroupId): Future[Option[Long]] =
    db.run {
      Schema.campaignStats
        .filter(_.campaignId === campaign)
        .filter(_.groupId === group)
        .filter(!_.completed)
        .map(_.processed)
        .result
        .headOption
    }

  private[db] def progressGroup(ns: Namespace,
                                campaign: CampaignId,
                                group: GroupId,
                                complete: Boolean,
                                stats: Stats): Future[Unit] =
    db.run {
      find(ns, campaign).flatMap { _ =>
        Schema.campaignStats
          .insertOrUpdate(CampaignStats(campaign, group, complete, stats.processed, stats.affected))
          .map(_ => ())
      }
    }

  def completeBatch(ns: Namespace,
                    campaign: CampaignId,
                    group: GroupId,
                    stats: Stats): Future[Unit] =
    progressGroup(ns, campaign, group, false, stats)

  def completeGroup(ns: Namespace,
                    campaign: CampaignId,
                    group: GroupId,
                    stats: Stats): Future[Unit] =
    progressGroup(ns, campaign, group, true, stats)

}
