package com.advancedtelematic.campaigner.actor

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.CampaignSupport
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import scala.concurrent.ExecutionContext

class CampaignSupervisorSpec extends TestKit(ActorSystem("CampaignSupervisorSpec"))
  with CampaignSupport
  with FlatSpecLike
  with Matchers
  with ScalaFutures
  with Settings
  with BeforeAndAfterAll
  with DatabaseSpec {

  import Arbitrary._
  import CampaignSupervisor._
  import scala.concurrent.duration._

  implicit lazy val ec: ExecutionContext = system.dispatcher
  lazy val registry = new FakeDeviceRegistryClient()
  lazy val director = new FakeDirectorClient()

  "campaign supervisor" should "pick up unfinished and fresh campaigns" in {

    val campaign1 = arbitrary[Campaign].sample.get
    val campaign2 = arbitrary[Campaign].sample.get
    val group    = GroupId.generate
    val parent   = TestProbe()

    Campaigns.persist(campaign1, Set(group)).futureValue
    Campaigns.persist(campaign2, Set(group)).futureValue

    Campaigns.completeBatch(
      campaign1.namespace,
      campaign1.id,
      group,
      Stats(Gen.posNum[Long].sample.get,
            Gen.posNum[Long].sample.get)
    ).futureValue

    parent.childActorOf(CampaignSupervisor.props(
      registry,
      director
    ))

    parent.expectMsg(3.seconds, CampaignsScheduled(Set(campaign1.id)))

    Campaigns.scheduleGroups(campaign2.namespace, campaign2.id, Set(group)).futureValue

    parent.expectMsg(3.seconds, CampaignsScheduled(Set(campaign2.id)))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
    system.terminate()
  }

}

