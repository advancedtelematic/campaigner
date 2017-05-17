package com.advancedtelematic.campaigner.data

import com.advancedtelematic.libats.codecs.CirceEnum
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libats.data.UUIDKey.{UUIDKey, UUIDKeyObj}
import com.advancedtelematic.libats.slick.codecs.SlickEnum
import java.time.Instant
import java.util.UUID

object DataType {

  final case class CampaignId(uuid: UUID) extends UUIDKey
  object CampaignId extends UUIDKeyObj[CampaignId]

  final case class DeviceId(uuid: UUID) extends UUIDKey
  object DeviceId extends UUIDKeyObj[DeviceId]

  final case class GroupId(uuid: UUID) extends UUIDKey
  object GroupId extends UUIDKeyObj[GroupId]

  final case class UpdateId(uuid: UUID) extends UUIDKey
  object UpdateId extends UUIDKeyObj[UpdateId]

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
    val scheduled, launched, cancelled = Value
  }

  object CampaignStatus extends CirceEnum with SlickEnum {
    val prepared, scheduled, launched, finished, cancelled, failed = Value
  }

  final case class GroupStats(
    id: CampaignId,
    group: GroupId,
    status: GroupStatus.Value,
    processed: Long,
    affected: Long
  )

  final case class CampaignStats(
    campaign: CampaignId,
    status: CampaignStatus.Value,
    stats: Map[GroupId, Stats]
  )

}
