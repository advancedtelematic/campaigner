package com.advancedtelematic.campaigner.db

import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libats.slick.db.SlickAnyVal._
import com.advancedtelematic.libats.slick.db.SlickExtensions._
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._
import java.time.Instant
import slick.jdbc.MySQLProfile.api._

object Schema {

  class Campaigns(tag: Tag) extends Table[Campaign](tag, "campaigns") {
    def namespace = column[Namespace] ("namespace")
    def id        = column[CampaignId]("uuid", O.PrimaryKey)
    def name      = column[String]    ("name")
    def update    = column[UpdateId]  ("update_id")
    def createdAt = column[Instant]   ("created_at")
    def updatedAt = column[Instant]   ("updated_at")

    override def * = (namespace, id, name, update, createdAt, updatedAt) <>
                     ((Campaign.apply _).tupled, Campaign.unapply)
  }

  protected [db] val campaigns = TableQuery[Campaigns]


  class CampaignGroups(tag: Tag) extends Table[(CampaignId, GroupId)](tag, "campaign_groups") {
    def campaignId = column[CampaignId]("campaign_id")
    def groupId    = column[GroupId]("group_id")

    def pk = primaryKey("campaign_groups_pk", (campaignId, groupId))

    override def * = (campaignId, groupId)
  }

  protected [db] val campaignGroups = TableQuery[CampaignGroups]


  class GroupStatsTable(tag: Tag) extends Table[GroupStats](tag, "group_stats") {
    def campaignId = column[CampaignId]("campaign_id")
    def groupId    = column[GroupId]("group_id")
    def status     = column[GroupStatus.Value]("status")
    def processed  = column[Long]("processed")
    def affected   = column[Long]("affected")

    def pk = primaryKey("group_stats_pk", (campaignId, groupId))

    override def * = (campaignId, groupId, status, processed, affected) <>
                     ((GroupStats.apply _).tupled, GroupStats.unapply)
  }

  protected [db] val groupStats = TableQuery[GroupStatsTable]

}
