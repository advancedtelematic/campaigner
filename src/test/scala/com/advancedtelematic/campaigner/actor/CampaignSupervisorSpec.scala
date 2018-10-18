package com.advancedtelematic.campaigner.actor

import akka.actor.{PoisonPill, Terminated}
import akka.testkit.TestProbe
import cats.data.NonEmptyList
import com.advancedtelematic.campaigner.actor.CampaignScheduler.CampaignSchedulingComplete
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{CampaignErrorsSupport, Campaigns, UpdateSupport}
import com.advancedtelematic.campaigner.util.{ActorSpec, CampaignerSpec}
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach

import scala.async.Async._
import scala.concurrent.Future
import scala.concurrent.duration._

class CampaignSupervisorSpec extends ActorSpec[CampaignSupervisor] with BeforeAndAfterEach with CampaignerSpec with UpdateSupport with CampaignErrorsSupport {

  import CampaignSupervisor._
  import org.scalacheck.Arbitrary._

  val campaigns = Campaigns()

  def buildCampaignWithUpdate: Campaign = {
    val update = genMultiTargetUpdate.sample.get
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

    val child = parent.childActorOf(CampaignSupervisor.props(
      deviceRegistry,
      director,
      schedulerPollingTimeout,
      schedulerDelay,
      schedulerBatchSize
    ))

    parent.expectMsg(3.seconds, CampaignsScheduled(Set(campaign1.id)))
    parent.expectMsg(3.seconds, CampaignSchedulingComplete(campaign1.id))

    campaigns.scheduleGroups(campaign2.id, group).futureValue

    parent.expectMsg(3.seconds, CampaignsScheduled(Set(campaign2.id)))
    parent.expectMsg(3.seconds, CampaignSchedulingComplete(campaign2.id))

    child ! PoisonPill
  }

  it should "campaigns with enough errors" in {
    val campaign = buildCampaignWithUpdate
    val group    = NonEmptyList.one(GroupId.generate)
    val parent   = TestProbe()

    async {
      await(campaigns.create(campaign, group, Seq.empty))
      await(campaigns.scheduleGroups(campaign.id, group))
      val errorsF = (0 to CampaignScheduler.MAX_CAMPAIGN_ERROR_COUNT).map(i => campaignErrorsRepo.addError(campaign.id, s"Some error $i"))
      await(Future.sequence(errorsF))
    }.futureValue

    val child = parent.childActorOf(CampaignSupervisor.props(
      deviceRegistry,
      director,
      schedulerPollingTimeout,
      10.seconds,
      schedulerBatchSize
    ), "CampaignSupervisorIgnoreErrors")

    parent.expectNoMessage(3.seconds)

    child ! PoisonPill
  }

  it should "clean out campaigns that are marked to be cancelled" in {
    val campaign = buildCampaignWithUpdate
    val group    = NonEmptyList.one(GroupId.generate)
    val parent   = TestProbe()
    val n        = Gen.choose(batch, batch * 2).sample.get
    val devs     = Gen.listOfN(n, genDeviceId).sample.get

    deviceRegistry.setGroup(group.head, devs)

    campaigns.create(campaign, group, Seq.empty).futureValue
    campaigns.scheduleGroups(campaign.id, group).futureValue

    val child = parent.childActorOf(CampaignSupervisor.props(
      deviceRegistry,
      director,
      schedulerPollingTimeout,
      10.seconds,
      schedulerBatchSize
    ), "CampaignSupervisorCancel")

    parent.expectMsg(5.seconds, CampaignsScheduled(Set(campaign.id)))

    campaigns.cancel(campaign.id).futureValue

    parent.expectMsg(5.seconds, CampaignsCancelled(Set(campaign.id)))

    child ! PoisonPill
  }
}
