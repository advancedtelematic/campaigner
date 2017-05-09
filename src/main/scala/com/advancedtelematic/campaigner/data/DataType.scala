package com.advancedtelematic.campaigner.data

import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libats.data.UUIDKey.{UUIDKey, UUIDKeyObj}
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
    id: CampaignId,
    namespace: Namespace,
    name: String,
    updateId: UpdateId,
    createdAt: Instant,
    updatedAt: Instant
  )

  final case class CreateCampaign(
    namespace: Namespace,
    name: String,
    update: UpdateId,
    groups: Set[GroupId]
  ) {
    def mkCampaign(): Campaign =
      Campaign(
        CampaignId.generate(),
        namespace,
        name,
        update,
        Instant.now(),
        Instant.now()
      )
  }

  final case class GetCampaign(
    id: CampaignId,
    namespace: Namespace,
    name: String,
    update: UpdateId,
    createdAt: Instant,
    updatedAt: Instant,
    groups: Set[GroupId]
  )

  object GetCampaign {
    def apply(c: Campaign, grps: Set[GroupId]): GetCampaign =
      GetCampaign(
        c.id,
        c.namespace,
        c.name,
        c.updateId,
        c.createdAt,
        c.updatedAt,
        grps
      )
  }

  final case class UpdateCampaign(
    name: String
  )

}
