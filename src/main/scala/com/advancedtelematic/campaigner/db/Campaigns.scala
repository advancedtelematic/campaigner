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
        _ <- Schema.Campaigns += campaign
        _ <- Schema.CampaignGroups ++= groups.map(g => (campaign.id, g))
      } yield ()).transactionally
    }

  def find(id: CampaignId): Future[Campaign] =
    db.run {
      Schema.Campaigns
        .filter(_.id === id)
        .result
        .failIfNotSingle(NotFound)
    }

  def findGroups(id: CampaignId): Future[Set[GroupId]] = {
    db.run {
      Schema.CampaignGroups
        .filter(_.campaignId === id)
        .map(_.groupId)
        .result
        .map(_.toSet)
    }
  }

  def update(id: CampaignId, name: String): Future[Unit] = {
    db.run {
      Schema.Campaigns
        .filter(_.id === id)
        .map(c => (c.name, c.updatedAt))
        .update((name, Instant.now()))
        .map(_ => ())
    }
  }

}
