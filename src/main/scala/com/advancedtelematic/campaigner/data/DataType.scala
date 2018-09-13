package com.advancedtelematic.campaigner.data

import java.time.Instant
import java.util.UUID

import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType.CancelTaskStatus.CancelTaskStatus
import com.advancedtelematic.campaigner.data.DataType.DeviceStatus.DeviceStatus
import com.advancedtelematic.campaigner.data.DataType.GroupStatus.GroupStatus
import com.advancedtelematic.campaigner.data.DataType.MetadataType.MetadataType
import com.advancedtelematic.campaigner.data.DataType.UpdateType.UpdateType
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.UUIDKey.{UUIDKey, UUIDKeyObj}
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}

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
    status: CampaignStatus,
    createdAt: Instant,
    updatedAt: Instant,
    autoAccept: Boolean = true
  )
  
  final case class ExternalUpdateId(value: String) extends AnyVal

  final case class UpdateSource(id: ExternalUpdateId, sourceType: UpdateType)

  final case class Update(
                           uuid: UpdateId,
                           source: UpdateSource,
                           namespace: Namespace,
                           name: String,
                           description: Option[String],
                           createdAt: Instant,
                           updatedAt: Instant
                         )

  final case class CreateUpdate(updateSource: UpdateSource, name: String, description: Option[String]) {
    def mkUpdate(ns: Namespace): Update =
      Update(UpdateId.generate(), updateSource, ns, name, description, Instant.now, Instant.now)
  }

  case class CampaignMetadata(campaignId: CampaignId, `type`: MetadataType, value: String)

  case class CreateCampaignMetadata(`type`: MetadataType, value: String) {
    def toCampaignMetadata(campaignId: CampaignId) = CampaignMetadata(campaignId, `type`, value)
  }

  final case class CreateCampaign(name: String,
                                  update: UpdateId,
                                  groups: Set[GroupId],
                                  metadata: Option[Seq[CreateCampaignMetadata]] = None,
                                  approvalNeeded: Option[Boolean] = Some(false))
  {
    def mkCampaign(ns: Namespace): Campaign = {
      Campaign(
        ns,
        CampaignId.generate(),
        name,
        update,
        CampaignStatus.prepared,
        Instant.now(),
        Instant.now(),
        !approvalNeeded.getOrElse(false)
      )
    }

    def mkCampaignMetadata(campaignId: CampaignId): Seq[CampaignMetadata] =
      metadata.toList.flatten.map(_.toCampaignMetadata(campaignId))
  }

  case class GetDeviceCampaigns(deviceId: DeviceId, campaigns: Seq[DeviceCampaign])

  case class DeviceCampaign(id: CampaignId, name: String, metadata: Seq[CreateCampaignMetadata])

  final case class GetCampaign(
    namespace: Namespace,
    id: CampaignId,
    name: String,
    update: UpdateId,
    status: CampaignStatus,
    createdAt: Instant,
    updatedAt: Instant,
    groups: Set[GroupId],
    metadata: Seq[CreateCampaignMetadata],
    autoAccept: Boolean
  )

  object GetCampaign {
    def apply(c: Campaign, groups: Set[GroupId], metadata: Seq[CampaignMetadata]): GetCampaign =
      GetCampaign(
        c.namespace,
        c.id,
        c.name,
        c.updateId,
        c.status,
        c.createdAt,
        c.updatedAt,
        groups,
        metadata.map(m => CreateCampaignMetadata(m.`type`, m.value)),
        c.autoAccept
      )
  }

  final case class UpdateCampaign(name: String, metadata: Option[Seq[CreateCampaignMetadata]] = None)

  final case class Stats(processed: Long, affected: Long)

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

  object SortBy {
    sealed trait SortBy
    case object Name      extends SortBy
    case object CreatedAt extends SortBy
  }

  object GroupStatus extends Enumeration {
    type GroupStatus = Value
    val scheduled, launched, cancelled = Value
  }

  object CampaignStatus extends Enumeration {
    type CampaignStatus = Value
    val prepared, launched, finished, cancelled = Value
  }

  object DeviceStatus extends Enumeration {
    type DeviceStatus = Value
    val scheduled, accepted, successful, cancelled, failed = Value
  }

  object CancelTaskStatus extends Enumeration {
    type CancelTaskStatus = Value
    val error, pending, inprogress, completed = Value
  }

  object UpdateType extends Enumeration {
    type UpdateType = Value
    val external, multi_target = Value
  }

  object MetadataType extends Enumeration {
    type MetadataType = Value
    val DESCRIPTION, ESTIMATED_INSTALLATION_DURATION, ESTIMATED_PREPARATION_DURATION = Value
  }
}
