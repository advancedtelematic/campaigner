package com.advancedtelematic.campaigner.db

import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.Namespace
import java.time.Instant
import slick.driver.MySQLDriver.api._

object Schema {
  import com.advancedtelematic.libats.slick.db.SlickAnyVal._
  import com.advancedtelematic.libats.slick.db.SlickExtensions._
  import com.advancedtelematic.libats.slick.db.SlickUUIDKey._

  class Campaigns(tag: Tag) extends Table[Campaign](tag, "campaigns") {
    def        id = column[CampaignId]("uuid", O.PrimaryKey)
    def namespace = column[Namespace] ("namespace")
    def      name = column[String]    ("name")
    def    update = column[UpdateId]  ("update_id")
    def createdAt = column[Instant]   ("created_at")
    def updatedAt = column[Instant]   ("updated_at")

    override def * = (id, namespace, name, update, createdAt, updatedAt) <>
                     ((Campaign.apply _).tupled, Campaign.unapply)
  }

  protected [db] val Campaigns = TableQuery[Campaigns]

  class CampaignGroups(tag: Tag) extends Table[(CampaignId, GroupId)](tag, "campaign_groups") {
    def campaignId = column[CampaignId]("campaign_id")
    def groupId    = column[GroupId]("group_id")

    def pk = primaryKey("pk", (campaignId, groupId))

    override def * = (campaignId, groupId)
  }

  protected [db] val CampaignGroups = TableQuery[CampaignGroups]
}
