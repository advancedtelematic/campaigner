package com.advancedtelematic.campaigner.actor

import akka.testkit.TestProbe
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.campaigner.util.{ActorSpec, CampaignerSpec}
import org.scalacheck.Arbitrary

import scala.collection.JavaConverters._

class CampaignSchedulerSpec extends ActorSpec[CampaignScheduler] with CampaignerSpec {

  import Arbitrary._
  import CampaignScheduler._
  import scala.concurrent.duration._

  val campaigns = Campaigns()

  "campaign scheduler" should "trigger updates for each group" in {

    val campaign = arbitrary[Campaign].sample.get
    val groups   = arbitrary[Set[GroupId]].sample.get
    val parent   = TestProbe()

    campaigns.create(campaign, groups).futureValue
    campaigns.scheduleGroups(campaign.namespace, campaign.id, groups).futureValue

    parent.childActorOf(CampaignScheduler.props(
      registry,
      director,
      campaign,
      schedulerDelay,
      schedulerBatchSize
    ))
    parent.expectMsg(1.minute, CampaignComplete(campaign.id))
    registry.state.keys.asScala.toSet shouldBe groups
  }

}
