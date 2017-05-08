package com.advancedtelematic.campaigner.actor

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfterAll, Matchers, PropSpecLike}
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext}

class CampaignSchedulerSpec extends TestKit(ActorSystem("CampaignSchedulerSpec"))
  with Matchers
  with PropertyChecks
  with PropSpecLike
  with Settings
  with BeforeAndAfterAll {

  import CampaignScheduler._
  import StatsCollector._
  import akka.pattern.ask
  import scala.concurrent.duration._

  implicit lazy val ec: ExecutionContext = system.dispatcher
  lazy val registry = new FakeDeviceRegistryClient()
  lazy val director = new FakeDirectorClient()

  def clearClientState()(implicit ec: ExecutionContext) {
    registry.state.clear()
    director.state.clear()
  }

  property("campaign scheduler should trigger updates for each group") {
    forAll(minSuccessful(3)) { (campaign: Campaign,
                                grps: Set[GroupId]) =>

      implicit val timeout = Timeout(3.seconds)
      clearClientState()

      val parent = TestProbe()
      val collector = TestActorRef[StatsCollector]
      collector ! Start()
      val props = CampaignScheduler.props(
        registry,
        director,
        collector,
        campaign
      )
      val ref = parent.childActorOf(props)
      val r = Await.result(ref ? ScheduleCampaign(grps), timeout.duration).asInstanceOf[CampaignScheduled]
      r.campaign shouldBe campaign.id
      parent.expectMsg(1.minutes, CampaignComplete(campaign.id))
      registry.state.keys.asScala.toSet shouldBe grps
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
    system.terminate()
    ()
  }

}
