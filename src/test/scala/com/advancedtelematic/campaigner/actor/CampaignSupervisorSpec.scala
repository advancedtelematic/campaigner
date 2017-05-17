package com.advancedtelematic.campaigner.actor

import akka.actor.ActorSystem
import akka.http.scaladsl.util.FastFuture
import akka.testkit.{TestKit, TestProbe}
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.CampaignSupport
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import scala.concurrent.duration._
import scala.concurrent.{Future, ExecutionContext}

trait CampaignSupervisorSpec extends CampaignSupport
  with FlatSpecLike
  with Matchers
  with ScalaFutures
  with Settings
  with BeforeAndAfterAll
  with DatabaseSpec {

  self: TestKit =>

  implicit lazy val ec: ExecutionContext = system.dispatcher
  lazy val registry = new FakeDeviceRegistryClient()
  lazy val director = new FakeDirectorClient()
  val batch = schedulerBatchSize.toInt

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
    system.terminate()
  }

}


class CampaignSupervisorSpec1 extends TestKit(ActorSystem("CampaignSupervisorSpec1"))
  with CampaignSupervisorSpec {

  import Arbitrary._
  import CampaignScheduler._
  import CampaignSupervisor._

  "campaign supervisor" should "pick up unfinished and fresh campaigns" in {
    val campaign1 = arbitrary[Campaign].sample.get
    val campaign2 = arbitrary[Campaign].sample.get
    val group    = GroupId.generate
    val parent   = TestProbe()

    Campaigns.persist(campaign1, Set(group)).futureValue
    Campaigns.persist(campaign2, Set(group)).futureValue

    Campaigns.scheduleGroups(campaign1.namespace, campaign1.id, Set(group)).futureValue
  
    parent.childActorOf(CampaignSupervisor.props(
      registry,
      director,
      schedulerPollingTimeout,
      schedulerDelay,
      schedulerBatchSize
    ))

    parent.expectMsg(3.seconds, CampaignsScheduled(Set(campaign1.id)))
    parent.expectMsg(3.seconds, CampaignComplete(campaign1.id))

    Campaigns.scheduleGroups(campaign2.namespace, campaign2.id, Set(group)).futureValue

    parent.expectMsg(3.seconds, CampaignsScheduled(Set(campaign2.id)))
  }

}


class CampaignSupervisorSpec2 extends TestKit(ActorSystem("CampaignSupervisorSpec2"))
  with CampaignSupervisorSpec {

  import Arbitrary._
  import CampaignSupervisor._

  "campaign supervisor" should "clean out campaigns that are marked to be cancelled" in {
    val campaign = arbitrary[Campaign].sample.get
    val group    = GroupId.generate
    val parent   = TestProbe()
    val n        = Gen.choose(batch, batch * 2).sample.get
    val devs     = Gen.listOfN(n, genDeviceId).sample.get
    val registry = new DeviceRegistryClient {
      override def devicesInGroup(_ns: Namespace,
                                  _grp: GroupId,
                                  offset: Long,
                                  limit: Long): Future[Seq[DeviceId]] =
        FastFuture.successful(devs.drop(offset.toInt).take(limit.toInt))
    }

    Campaigns.persist(campaign, Set(group)).futureValue
    Campaigns.scheduleGroups(campaign.namespace, campaign.id, Set(group))

    parent.childActorOf(CampaignSupervisor.props(
      registry,
      director,
      schedulerPollingTimeout,
      10.seconds,
      schedulerBatchSize
    ))

    parent.expectMsg(1.seconds, CampaignsScheduled(Set(campaign.id)))

    parent.expectNoMsg(1.seconds)
    Campaigns.cancelCampaign(campaign.namespace, campaign.id).futureValue

    parent.expectMsg(2.seconds, CampaignsCancelled(Set(campaign.id)))
    parent.expectNoMsg(2.seconds)
  }

}
