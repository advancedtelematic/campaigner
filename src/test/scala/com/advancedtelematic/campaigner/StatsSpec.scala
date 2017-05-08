package com.advancedtelematic.campaigner

import cats.Monoid
import com.advancedtelematic.campaigner.actor.StatsCollector._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.util.CampaignerSpec
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

class StatsMonoidalSpec extends CampaignerSpec {

  def monotonicityProperty(lhs: GroupStats, rhs: GroupStats)
      (implicit m1: Monoid[Stats], m2: Monoid[GroupStats]) = {
    val merged = m2.combine(lhs, rhs)
    (lhs.keySet union rhs.keySet).foreach { k =>
      val m = merged(k)
      val l = lhs.getOrElse(k, m1.empty)
      val r = rhs.getOrElse(k, m1.empty)
      m.processed shouldBe (l.processed + r.processed)
      m.affected  shouldBe (l.affected  + r.affected)
    }
  }

  def specifityProperty[K, V](lhs: Map[K, V], rhs: Map[K, V])
      (implicit m: Monoid[Map[K, V]]): Map[K, V] = {
    val merged = m.combine(lhs, rhs)
    merged.keySet shouldBe (lhs.keySet union rhs.keySet)
    merged
  }

  property("aggregating group stats is monotonic") {
    forAll(arbitrary[Seq[GroupId]].suchThat(!_.isEmpty)) { keys: Seq[GroupId] =>
      forAll(Gen.mapOf(Gen.zip(Gen.oneOf(keys), arbitrary[Stats])),
             Gen.mapOf(Gen.zip(Gen.oneOf(keys), arbitrary[Stats]))) {
        (lhs: GroupStats,
         rhs: GroupStats) =>

        monotonicityProperty(lhs, rhs)
      }
    }
  }

  property("aggregating group stats covers all groups") {
    forAll(arbitrary[Seq[GroupId]].suchThat(!_.isEmpty)) { keys: Seq[GroupId] =>
      forAll(Gen.mapOf(Gen.zip(Gen.oneOf(keys), arbitrary[Stats])),
             Gen.mapOf(Gen.zip(Gen.oneOf(keys), arbitrary[Stats]))) {
        (lhs: GroupStats,
         rhs: GroupStats) =>

        specifityProperty(lhs, rhs)
      }
    }
  }

  property("aggregating campaign stats is monotonic") {
    forAll(arbitrary[Seq[CampaignId]].suchThat(!_.isEmpty)) { keys: Seq[CampaignId] =>
      forAll(Gen.mapOf(Gen.zip(Gen.oneOf(keys), arbitrary[GroupStats])),
             Gen.mapOf(Gen.zip(Gen.oneOf(keys), arbitrary[GroupStats]))) {
        (lhs: CampaignStats,
         rhs: CampaignStats) =>

        val merged = implicitly[Monoid[CampaignStats]].combine(lhs, rhs)
        merged.keySet.foreach { campaign =>
          val m = merged(campaign)
          val l = lhs.getOrElse(campaign, Map.empty)
          val r = rhs.getOrElse(campaign, Map.empty)
          m.keySet.foreach { group =>
            val stats  = m(group)
            val lstats = l.getOrElse(group, Stats(0, 0))
            val rstats = r.getOrElse(group, Stats(0, 0))
            stats.processed shouldBe (lstats.processed + rstats.processed)
            stats.affected  shouldBe (lstats.affected  + rstats.affected)
          }
        }
      }
    }
  }

  property("aggregating campaign stats covers all groups") {
    forAll(arbitrary[Seq[CampaignId]].suchThat(!_.isEmpty)) { keys: Seq[CampaignId] =>
      forAll(Gen.mapOf(Gen.zip(Gen.oneOf(keys), arbitrary[GroupStats])),
             Gen.mapOf(Gen.zip(Gen.oneOf(keys), arbitrary[GroupStats]))) {
        (lhs: CampaignStats,
         rhs: CampaignStats) =>

        val merged = specifityProperty(lhs, rhs)
        merged.keySet.foreach { key =>
          merged(key).keySet shouldBe (lhs.getOrElse(key, Map.empty).keySet union
                                       rhs.getOrElse(key, Map.empty).keySet)
        }
      }
    }
  }

}
