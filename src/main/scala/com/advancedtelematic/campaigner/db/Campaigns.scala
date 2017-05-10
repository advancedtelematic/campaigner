package com.advancedtelematic.campaigner.db

import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.http.Errors.MissingEntity
import com.advancedtelematic.libats.slick.db.SlickExtensions._
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}
import slick.driver.MySQLDriver.api._

trait CampaignSupport {
  def Campaigns(implicit db: Database, ec: ExecutionContext) = new Campaigns()
}

protected class Campaigns()(implicit db: Database, ec: ExecutionContext) {

  val NotFound = MissingEntity[Campaign]

  def persist(campaign: Campaign, groups: Set[GroupId]): Future[Unit] =
    db.run {
      (for {
        _ <- Schema.campaigns += campaign
        _ <- Schema.campaignGroups ++= groups.map(g => (campaign.id, g))
      } yield ()).transactionally
    }

  def find(id: CampaignId): Future[Campaign] =
    db.run {
      Schema.campaigns
        .filter(_.id === id)
        .result
        .failIfNotSingle(NotFound)
    }

  def findGroups(id: CampaignId): Future[Set[GroupId]] =
    db.run {
      Schema.campaignGroups
        .filter(_.campaignId === id)
        .map(_.groupId)
        .result
        .map(_.toSet)
    }

  def update(id: CampaignId, name: String): Future[Unit] =
    db.run {
      Schema.campaigns
        .filter(_.id === id)
        .map(c => (c.name, c.updatedAt))
        .update((name, Instant.now()))
        .map(_ => ())
    }

  def campaignStats(): Future[Map[Campaign, Seq[(GroupId, Stats)]]] =
    db.run {
      (Schema.campaignStats join Schema.campaigns on (_.campaignId === _.id))
        .map(r => (r._2, r._1.groupId, r._1.processed, r._1.affected))
        .result
    }.map(_.groupBy(_._1).map { case (k, v) => (k, v.map(r => (r._2, Stats(r._3, r._4)))) })

  def uncompletedCampaigns(): Future[Map[Campaign, Seq[(GroupId, Int)]]] =
    db.run {
      (Schema.campaignStats join Schema.campaigns on (_.campaignId === _.id))
        .filter(!_._1.completed)
        .map(r => (r._2, r._1.groupId, r._1.processed))
        .result
    }.map(_.groupBy(_._1).map { case (k, v) => (k, v.map(r => (r._2, r._3))) })

  private[db] def progressGroup(campaign: CampaignId,
                                grp: GroupId,
                                complete: Boolean,
                                stats: Stats): Future[Unit] =
    db.run {
      Schema.campaignStats
        .insertOrUpdate(CampaignStats(campaign, grp, complete, stats.processed, stats.affected))
        .map(_ => ())
    }

  def completeBatch(campaign: CampaignId,
                    grp: GroupId,
                    stats: Stats): Future[Unit] =
    progressGroup(campaign, grp, false, stats)

  def completeGroup(campaign: CampaignId,
                    grp: GroupId,
                    stats: Stats): Future[Unit] =
    progressGroup(campaign, grp, true, stats)

}
