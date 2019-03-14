package com.advancedtelematic.campaigner.actor

import akka.testkit.TestProbe
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, UpdateSupport}
import com.advancedtelematic.campaigner.util.{ActorSpec, CampaignerSpec}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen

import scala.concurrent.duration._

class CampaignSupervisorSpec extends ActorSpec[CampaignSupervisor] with CampaignerSpec with UpdateSupport {

  import CampaignScheduler._
  import CampaignSupervisor._

  val campaigns = Campaigns()

  def buildCampaignWithUpdate: Campaign = {
    val update = genMultiTargetUpdate.generate
    val updateId = updateRepo.persist(update).futureValue
    arbitrary[Campaign].generate.copy(updateId = updateId)
  }

  "campaign supervisor" should "pick up unfinished and fresh campaigns" in {
    val campaign1 = buildCampaignWithUpdate
    val campaign2 = buildCampaignWithUpdate
    val parent    = TestProbe()

    val n = Gen.choose(batch, batch * 2).generate
    val devices1 = Gen.listOfN(n, genDeviceId).generate.toSet
    val devices2 = Gen.listOfN(n, genDeviceId).generate.toSet

    campaigns.create(campaign1, Set.empty, devices1, Seq.empty).futureValue

    parent.childActorOf(CampaignSupervisor.props(
      director,
      schedulerPollingTimeout,
      schedulerDelay,
      schedulerBatchSize
    ))

    parent.expectMsg(3.seconds, CampaignsScheduled(Set(campaign1.id)))
    parent.expectMsg(3.seconds, CampaignComplete(campaign1.id))

    campaigns.create(campaign2, Set.empty, devices2, Seq.empty).futureValue
    parent.expectMsg(3.seconds, CampaignsScheduled(Set(campaign2.id)))
  }
}

class CampaignSupervisorSpec2 extends ActorSpec[CampaignSupervisor] with CampaignerSpec with UpdateSupport {

  import CampaignSupervisor._
  import org.scalacheck.Arbitrary._

  val campaigns = Campaigns()

  def buildCampaignWithUpdate: Campaign = {
    val update = genMultiTargetUpdate.generate
    val updateId = updateRepo.persist(update).futureValue
    arbitrary[Campaign].generate.copy(updateId = updateId)
  }

  "campaign supervisor" should "clean out campaigns that are marked to be cancelled" in {
    val campaign = buildCampaignWithUpdate
    val parent   = TestProbe()
    val n        = Gen.choose(batch, batch * 2).generate
    val devs     = Gen.listOfN(n, genDeviceId).generate.toSet

    campaigns.create(campaign, Set.empty, devs, Seq.empty).futureValue

    parent.childActorOf(CampaignSupervisor.props(
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
