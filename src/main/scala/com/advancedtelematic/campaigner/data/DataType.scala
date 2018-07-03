package com.advancedtelematic.campaigner.data

import com.advancedtelematic.libats.codecs.CirceEnum
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.UUIDKey.{UUIDKey, UUIDKeyObj}
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import com.advancedtelematic.libats.slick.codecs.SlickEnum
import java.time.Instant
import java.util.UUID

import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType.CancelTaskStatus.CancelTaskStatus
import com.advancedtelematic.campaigner.data.DataType.DeviceStatus.DeviceStatus
import com.advancedtelematic.campaigner.data.DataType.GroupStatus.GroupStatus

object DataType {

  final case class CampaignId(uuid: UUID) extends UUIDKey
  object CampaignId extends UUIDKeyObj[CampaignId]

  final case class GroupId(uuid: UUID) extends UUIDKey
  object GroupId extends UUIDKeyObj[GroupId]

  final case class Campaign(
    namespace: Namespace,
    id: CampaignId,
    name: String,
    updateId: UpdateId,
    createdAt: Instant,
    updatedAt: Instant
  )

  final case class CreateCampaign(
    name: String,
    update: UpdateId,
    groups: Set[GroupId]
  ) {
    def mkCampaign(ns: Namespace): Campaign =
      Campaign(
        ns,
        CampaignId.generate(),
        name,
        update,
        Instant.now(),
        Instant.now()
      )
  }

  final case class GetCampaign(
    namespace: Namespace,
    id: CampaignId,
    name: String,
    update: UpdateId,
    createdAt: Instant,
    updatedAt: Instant,
    groups: Set[GroupId]
  )

  object GetCampaign {
    def apply(c: Campaign, groups: Set[GroupId]): GetCampaign =
      GetCampaign(
        c.namespace,
        c.id,
        c.name,
        c.updateId,
        c.createdAt,
        c.updatedAt,
        groups
      )
  }

  final case class UpdateCampaign(
    name: String
  )

  final case class Stats(processed: Long, affected: Long)

  object GroupStatus extends CirceEnum with SlickEnum {
    type GroupStatus = Value
    val scheduled, launched, cancelled = Value
  }

  object CampaignStatus extends CirceEnum {
    type CampaignStatus = Value
    val prepared, scheduled, launched, finished, cancelled = Value
  }

  object DeviceStatus extends Enumeration with SlickEnum {
    type DeviceStatus = Value
    val scheduled, successful, cancelled, failed = Value
  }

  object CancelTaskStatus extends Enumeration with SlickEnum {
    type CancelTaskStatus = Value
    val error, pending, inprogress, completed = Value
  }

  final case class GroupStats(
    campaign: CampaignId,
    group: GroupId,
    status: GroupStatus,
    processed: Long,
    affected: Long
  )

  final case class CampaignStats(
    campaign: CampaignId,
    status: CampaignStatus,
    finished: Long,
    failed: Set[DeviceId],
    cancelled: Long,
    stats: Map[GroupId, Stats]
  )

  final case class DeviceUpdate(
    campaign: CampaignId,
    update: UpdateId,
    device: DeviceId,
    status: DeviceStatus
  )

  final case class CancelTask(
    campaign: CampaignId,
    status: CancelTaskStatus
  )
}
