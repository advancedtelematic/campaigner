package com.advancedtelematic.campaigner.db

import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.slick.db.SlickAnyVal._
import com.advancedtelematic.libats.slick.db.SlickExtensions._
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import java.time.Instant

import com.advancedtelematic.campaigner.data.DataType.CancelTaskStatus.CancelTaskStatus
import com.advancedtelematic.campaigner.data.DataType.DeviceStatus.DeviceStatus
import com.advancedtelematic.campaigner.data.DataType.GroupStatus.GroupStatus
import slick.jdbc.MySQLProfile.api._

object Schema {

  class CampaignsTable(tag: Tag) extends Table[Campaign](tag, "campaigns") {
    def namespace = column[Namespace] ("namespace")
    def id        = column[CampaignId]("uuid", O.PrimaryKey)
    def name      = column[String]    ("name")
    def update    = column[UpdateId]  ("update_id")
    def createdAt = column[Instant]   ("created_at")
    def updatedAt = column[Instant]   ("updated_at")

    override def * = (namespace, id, name, update, createdAt, updatedAt) <>
                     ((Campaign.apply _).tupled, Campaign.unapply)
  }

  protected [db] val campaigns = TableQuery[CampaignsTable]


  // There is already an association between campaigns and groups in GroupStatsTable. Why do we need this?
  // If it's just for the campaign resource, we can have a new GroupStatus => created and create that when we create
  // a campaign
  class CampaignGroupsTable(tag: Tag) extends Table[(CampaignId, GroupId)](tag, "campaign_groups") {
    def campaignId = column[CampaignId]("campaign_id")
    def groupId    = column[GroupId]("group_id")

    def pk = primaryKey("campaign_groups_pk", (campaignId, groupId))

    override def * = (campaignId, groupId)
  }

  protected [db] val campaignGroups = TableQuery[CampaignGroupsTable]


  class GroupStatsTable(tag: Tag) extends Table[GroupStats](tag, "group_stats") {
    def campaignId = column[CampaignId]("campaign_id")
    def groupId    = column[GroupId]("group_id")
    def status     = column[GroupStatus]("status")
    def processed  = column[Long]("processed")
    def affected   = column[Long]("affected")

    def pk = primaryKey("group_stats_pk", (campaignId, groupId))

    override def * = (campaignId, groupId, status, processed, affected) <>
                     ((GroupStats.apply _).tupled, GroupStats.unapply)
  }

  protected [db] val groupStats = TableQuery[GroupStatsTable]


  class DeviceUpdatesTable(tag: Tag) extends Table[DeviceUpdate](tag, "device_updates") {
    def campaignId = column[CampaignId]("campaign_id")
    def updateId   = column[UpdateId]("update_id")
    def deviceId   = column[DeviceId]("device_id")
    def status     = column[DeviceStatus]("status")

    def pk = primaryKey("device_updates_pk", (campaignId, deviceId))

    override def * = (campaignId, updateId, deviceId, status) <>
                     ((DeviceUpdate.apply _).tupled, DeviceUpdate.unapply)
  }

  protected [db] val deviceUpdates = TableQuery[DeviceUpdatesTable]

  class CancelTaskTable(tag: Tag) extends Table[CancelTask](tag, "campaign_cancels") {
    def campaignId = column[CampaignId]("campaign_id", O.PrimaryKey)
    def taskStatus = column[CancelTaskStatus]("status")

    override def * = (campaignId, taskStatus) <>
      ((CancelTask.apply _).tupled, CancelTask.unapply)
  }
  protected [db] val cancelTasks = TableQuery[CancelTaskTable]
}
