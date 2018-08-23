package com.advancedtelematic.campaigner.actor

import akka.http.scaladsl.util.FastFuture
import akka.testkit.TestProbe
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.campaigner.util.{ActorSpec, CampaignerSpec}
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.duration._
import scala.concurrent.Future

class CampaignSupervisorSpec1 extends ActorSpec[CampaignSupervisor] with CampaignerSpec {

  import Arbitrary._
  import CampaignScheduler._
  import CampaignSupervisor._

  val campaigns = Campaigns()

  "campaign supervisor" should "pick up unfinished and fresh campaigns" in {
    val campaign1 = arbitrary[Campaign].sample.get
    val campaign2 = arbitrary[Campaign].sample.get
    val group     = GroupId.generate
    val parent    = TestProbe()

    campaigns.create(campaign1, Set(group), Seq.empty).futureValue
    campaigns.create(campaign2, Set(group), Seq.empty).futureValue

    campaigns.scheduleGroups(campaign1.namespace, campaign1.id, Set(group)).futureValue

    parent.childActorOf(CampaignSupervisor.props(
      deviceRegistry,
      director,
      schedulerPollingTimeout,
      schedulerDelay,
      schedulerBatchSize
    ))

    parent.expectMsg(3.seconds, CampaignsScheduled(Set(campaign1.id)))
    parent.expectMsg(3.seconds, CampaignComplete(campaign1.id))

    campaigns.scheduleGroups(campaign2.namespace, campaign2.id, Set(group)).futureValue

    parent.expectMsg(3.seconds, CampaignsScheduled(Set(campaign2.id)))
  }

}

class CampaignSupervisorSpec2 extends ActorSpec[CampaignSupervisor] with CampaignerSpec {

  import Arbitrary._
  import CampaignSupervisor._

  val campaigns = Campaigns()

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

    campaigns.create(campaign, Set(group), Seq.empty).futureValue
    campaigns.scheduleGroups(campaign.namespace, campaign.id, Set(group))

    parent.childActorOf(CampaignSupervisor.props(
      registry,
      director,
      schedulerPollingTimeout,
      10.seconds,
      schedulerBatchSize
    ))
    parent.expectMsg(2.seconds, CampaignsScheduled(Set(campaign.id)))
    expectNoMessage(2.seconds)

    campaigns.cancelCampaign(campaign.namespace, campaign.id).futureValue
    parent.expectMsg(2.seconds, CampaignsCancelled(Set(campaign.id)))
    expectNoMessage(2.seconds)
  }

}
