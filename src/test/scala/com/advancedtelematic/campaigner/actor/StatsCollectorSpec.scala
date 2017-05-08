package com.advancedtelematic.campaigner.actor

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import org.scalacheck.Arbitrary._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers, PropSpecLike}
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext}

class StatsCollectorFlatSpec extends TestKit(ActorSystem("StatsCollectorFlatSpec"))
  with Matchers
  with FlatSpecLike
  with BeforeAndAfterAll {

  import CampaignScheduler._
  import StatsCollector._
  import akka.pattern.ask
  import scala.concurrent.duration._

  implicit lazy val ec: ExecutionContext = system.dispatcher
  lazy val registry = new FakeDeviceRegistryClient()
  lazy val director = new FakeDirectorClient()
  implicit val timeout = Timeout(3.seconds)

  "collector" should "work in tandem with campaign scheduler" in {

    val campaign = arbitrary[Campaign].sample.get
    val grps     = arbitrary[Set[GroupId]].sample.get

    val parent    = TestProbe()
    val collector = TestActorRef[StatsCollector]
    val scheduler = parent.childActorOf(
                      CampaignScheduler.props(
                        registry,
                        director,
                        collector,
                        campaign
                      ))
    collector ! Start()

    // before
    val r1 = Await.result(collector ? Ask(campaign.id), timeout.duration).asInstanceOf[CampaignStatsResult]
    r1 shouldBe CampaignStatsResult(campaign.id, Map.empty)

    // while
    val r2 = Await.result(scheduler ? ScheduleCampaign(grps), timeout.duration).asInstanceOf[CampaignScheduled]
    r2.campaign shouldBe campaign.id

    // after
    parent.expectMsg(1.minutes, CampaignComplete(campaign.id))
    val r3 = Await.result(collector ? Ask(campaign.id), timeout.duration).asInstanceOf[CampaignStatsResult]
    r3.campaign shouldBe campaign.id
    r3.stats.keySet.foreach { grp =>
      r3.stats(grp).processed shouldBe registry.state.get(grp).length
    }
    r3.stats.values.map(_.affected).sum shouldBe director.state.asScala.getOrElse(campaign.updateId, Set()).size
  }

}

class StatsCollectorPropSpec extends TestKit(ActorSystem("StatsCollectorPropSpec"))
  with Matchers
  with PropertyChecks
  with PropSpecLike
  with BeforeAndAfterAll {

  import StatsCollector._
  import akka.pattern.ask
  import scala.concurrent.duration._

  implicit lazy val ec: ExecutionContext = system.dispatcher
  lazy val registry = new FakeDeviceRegistryClient()
  lazy val director = new FakeDirectorClient()
  implicit val timeout = Timeout(3.seconds)

  def clearClientState()(implicit ec: ExecutionContext) {
    registry.state.clear()
    director.state.clear()
  }

  property("collector should accept stats updates and process them correctly") {
    forAll { (campaign: CampaignId,
              group: GroupId,
              stats1: Stats,
              stats2: Stats) =>

      val ref  = TestActorRef[StatsCollector]
      ref ! Start()

      ref ! Collect(campaign, group, stats1)
      val r1 = Await.result(ref ? Ask(campaign), timeout.duration).asInstanceOf[CampaignStatsResult]
      r1.campaign     shouldBe campaign
      r1.stats(group) shouldBe stats1

      ref ! Collect(campaign, group, stats2)
      val r2 = Await.result(ref ? Ask(campaign), timeout.duration).asInstanceOf[CampaignStatsResult]
      r2.campaign     shouldBe campaign
      r2.stats(group) shouldBe Stats(stats1.processed + stats2.processed,
                                     stats1.affected  + stats2.affected)
    }
  }

  property("collector should accept stats updates for different campaigns and separate them correctly") {
    forAll { (campaign1: CampaignId,
              campaign2: CampaignId,
              group: GroupId,
              stats1: Stats,
              stats2: Stats) =>

      val ref  = TestActorRef[StatsCollector]
      ref ! Start()

      ref ! Collect(campaign1, group, stats1)
      ref ! Collect(campaign2, group, stats2)

      val r1 = Await.result(ref ? Ask(campaign1), timeout.duration).asInstanceOf[CampaignStatsResult]
      r1.campaign     shouldBe campaign1
      r1.stats(group) shouldBe stats1

      val r2 = Await.result(ref ? Ask(campaign2), timeout.duration).asInstanceOf[CampaignStatsResult]
      r2.campaign     shouldBe campaign2
      r2.stats(group) shouldBe stats2
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
    system.terminate()
    ()
  }

}
