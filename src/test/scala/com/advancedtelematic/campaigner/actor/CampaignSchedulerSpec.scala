package com.advancedtelematic.campaigner.actor

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.CampaignSupport
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalacheck.Arbitrary
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class CampaignSchedulerSpec extends TestKit(ActorSystem("CampaignSchedulerSpec"))
  with CampaignSupport
  with FlatSpecLike
  with Matchers
  with ScalaFutures
  with Settings
  with BeforeAndAfterAll
  with DatabaseSpec {

  import Arbitrary._
  import CampaignScheduler._
  import scala.concurrent.duration._

  implicit lazy val ec: ExecutionContext = system.dispatcher
  lazy val registry = new FakeDeviceRegistryClient()
  lazy val director = new FakeDirectorClient()

  "campaign scheduler" should "trigger updates for each group" in {

    val campaign = arbitrary[Campaign].sample.get
    val groups   = arbitrary[Set[GroupId]].sample.get
    val parent = TestProbe()
    val props = CampaignScheduler.props(
      registry,
      director,
      campaign,
      groups
    )
    Campaigns.persist(campaign, groups).futureValue
    parent.childActorOf(props)
    parent.expectMsg(1.minute, CampaignComplete(campaign.id))
    registry.state.keys.asScala.toSet shouldBe groups
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
    system.terminate()
  }

}
