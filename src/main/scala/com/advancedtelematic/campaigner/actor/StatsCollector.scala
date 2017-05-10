package com.advancedtelematic.campaigner.actor

import akka.actor.Actor
import akka.event.Logging
import cats.Monoid
import com.advancedtelematic.campaigner.data.DataType._

object StatsCollector {

  final case class Stats(processed: Long, affected: Long)
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

  type GroupStats = Map[GroupId, Stats]
  type CampaignStats = Map[CampaignId, GroupStats]
  final case class CampaignStatsResult(campaign: CampaignId, stats: GroupStats)

  final case class Start()
  final case class Ask(campaign: CampaignId)
  final case class Collect(campaign: CampaignId, group: GroupId, stats: Stats)

}

class StatsCollector() extends Actor {

  import StatsCollector._
  import context._

  val log = Logging(system, this)

  def collecting(stats: CampaignStats)
                (implicit m: Monoid[CampaignStats]): Receive = {
    case Collect(campaign, group, partial) =>
      log.debug(s"collecting stats for campaign $group: ${partial.processed} processed, ${partial.affected} affected")
      become(collecting(m.combine(stats, Map(campaign -> Map(group -> partial)))))
    case Ask(campaign) =>
      sender ! CampaignStatsResult(campaign, stats.getOrElse(campaign, Map.empty))
    case msg => log.info(s"unexpected message: $msg")
  }

  def receive: Receive = {
    case Start() => become(collecting(Map.empty))
    case msg => log.info(s"unexpected message: $msg")
  }

}
