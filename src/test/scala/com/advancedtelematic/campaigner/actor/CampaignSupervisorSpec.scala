package com.advancedtelematic.campaigner.actor

import akka.http.scaladsl.util.FastFuture
import akka.testkit.TestProbe
import cats.data.NonEmptyList
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, UpdateSupport}
import com.advancedtelematic.campaigner.util.{ActorSpec, CampaignerSpec}
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen

import scala.concurrent.Future
import scala.concurrent.duration._

class CampaignSupervisorSpec extends ActorSpec[CampaignSupervisor] with CampaignerSpec with UpdateSupport {

  import CampaignScheduler._
  import CampaignSupervisor._

  val campaigns = Campaigns()

  def buildCampaignWithUpdate: Campaign = {
    val updateSource = genUpdateSource.retryUntil(_.sourceType == UpdateType.multi_target).sample.get
    val update = genUpdate.sample.get.copy(source = updateSource)
    val updateId = updateRepo.persist(update).futureValue
    arbitrary[Campaign].sample.get.copy(updateId = updateId)
  }

  "campaign supervisor" should "pick up unfinished and fresh campaigns" in {
    val campaign1 = buildCampaignWithUpdate
    val campaign2 = buildCampaignWithUpdate
    val group     = NonEmptyList.one(GroupId.generate)
    val parent    = TestProbe()

    campaigns.create(campaign1, group, Seq.empty).futureValue
    campaigns.create(campaign2, group, Seq.empty).futureValue

    campaigns.scheduleGroups(campaign1.id, group).futureValue

    parent.childActorOf(CampaignSupervisor.props(
      deviceRegistry,
      director,
      schedulerPollingTimeout,
      schedulerDelay,
      schedulerBatchSize
    ))

    parent.expectMsg(3.seconds, CampaignsScheduled(Set(campaign1.id)))
    parent.expectMsg(3.seconds, CampaignComplete(campaign1.id))

    campaigns.scheduleGroups(campaign2.id, group).futureValue

    parent.expectMsg(3.seconds, CampaignsScheduled(Set(campaign2.id)))
  }

}

class CampaignSupervisorSpec2 extends ActorSpec[CampaignSupervisor] with CampaignerSpec with UpdateSupport {

  import CampaignSupervisor._
  import org.scalacheck.Arbitrary._

  val campaigns = Campaigns()

  def buildCampaignWithUpdate: Campaign = {
    val updateSource = genUpdateSource.retryUntil(_.sourceType == UpdateType.multi_target).sample.get
    val update = genUpdate.sample.get.copy(source = updateSource)
    val updateId = updateRepo.persist(update).futureValue
    arbitrary[Campaign].sample.get.copy(updateId = updateId)
  }

  "campaign supervisor" should "clean out campaigns that are marked to be cancelled" in {
    val campaign = buildCampaignWithUpdate
    val group    = NonEmptyList.one(GroupId.generate)
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

    campaigns.create(campaign, group, Seq.empty).futureValue
    campaigns.scheduleGroups(campaign.id, group)

    parent.childActorOf(CampaignSupervisor.props(
      registry,
      director,
      schedulerPollingTimeout,
      10.seconds,
      schedulerBatchSize
    ))
    parent.expectMsg(5.seconds, CampaignsScheduled(Set(campaign.id)))
    expectNoMessage(5.seconds)

    campaigns.cancel(campaign.id).futureValue
    parent.expectMsg(5.seconds, CampaignsCancelled(Set(campaign.id)))
    expectNoMessage(5.seconds)
  }

}
