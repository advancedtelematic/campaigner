package com.advancedtelematic.campaigner.data

import cats.Monoid
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
    update: UpdateId,
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
        c.update,
        c.createdAt,
        c.updatedAt,
        grps
      )
  }

  final case class UpdateCampaign(
    name: String
  )

  final case class Stats(processed: Int, affected: Int)
  implicit val statsMonoid: Monoid[Stats] = new Monoid[Stats] {
    def empty: Stats = Stats(0, 0)
    def combine(lhs: Stats, rhs: Stats): Stats =
      Stats(lhs.processed + rhs.processed, lhs.affected + rhs.affected)
  }

  implicit def combineMapMonoid[K, V]
      (implicit m: Monoid[V])
      : Monoid[Map[K, V]] = new Monoid[Map[K, V]] {
    def empty: Map[K, V] = Map.empty
    def combine(lhs: Map[K, V], rhs: Map[K, V]): Map[K, V] = {
      val inter = lhs.keySet intersect rhs.keySet
      (lhs -- inter) ++ (rhs -- inter) ++ inter.map { k =>
        k -> m.combine(lhs(k), rhs(k))
      }
    }
  }

  final case class CampaignStats(
    id: CampaignId,
    grp: GroupId,
    completed: Boolean,
    processed: Int,
    affected: Int
  )

}
