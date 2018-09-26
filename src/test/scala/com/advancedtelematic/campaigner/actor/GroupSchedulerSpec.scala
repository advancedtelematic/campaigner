package com.advancedtelematic.campaigner.actor

import akka.testkit.TestProbe
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, DeviceUpdateSupport, UpdateSupport}
import com.advancedtelematic.campaigner.util.{ActorSpec, CampaignerSpec}
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import org.scalacheck.{Arbitrary, Gen}

import scala.collection.JavaConverters._

class GroupSchedulerSpec extends ActorSpec[GroupScheduler] with CampaignerSpec with DeviceUpdateSupport with UpdateSupport {

  import Arbitrary._
  import GroupScheduler._

  import scala.concurrent.duration._

  def clearClientState() = {
    deviceRegistry.clear()
    director.updates.clear()
    director.cancelled.clear()
  }

  def buildCampaignWithUpdate: Campaign = {
    val update = genMultiTargetUpdate.sample.get
    val updateId = updateRepo.persist(update).futureValue
    arbitrary[Campaign].sample.get.copy(updateId = updateId)
  }

  val campaigns = Campaigns()

  "group scheduler" should "trigger updates for each device in batch" in {
    val campaign = buildCampaignWithUpdate
    val update = updateRepo.findById(campaign.updateId).futureValue
    val group    = GroupId.generate()
    val parent = TestProbe()

    val props  = GroupScheduler.props(deviceRegistry, director, 10.minutes, schedulerBatchSize, campaign, group)

    clearClientState()
    deviceRegistry.setGroup(group, arbitrary[Seq[DeviceId]].sample.get)
    campaigns.create(campaign, Set(group), Seq.empty).futureValue

    parent.childActorOf(props)

    parent.expectMsgPF(10.seconds) {
      case BatchComplete(grp, _) => grp
      case GroupComplete(grp) => grp
    }

    deviceRegistry.allGroupDevices(group).take(batch) should contain allElementsOf director.updates.get(update.source.id)
  }

  "group scheduler" should "respect groups with processed devices > batch size" in {
    val campaign = buildCampaignWithUpdate
    val update = updateRepo.findById(campaign.updateId).futureValue
    val group    = GroupId.generate()
    val n        = Gen.choose(batch, batch * 10).sample.get
    val devs     = Gen.listOfN(n, genDeviceId).sample.get

    campaigns.create(campaign, Set(group), Seq.empty).futureValue

    clearClientState()
    deviceRegistry.setGroup(group, devs)

    val parent = TestProbe()
    val props  = GroupScheduler.props(deviceRegistry, director, schedulerDelay, schedulerBatchSize, campaign, group)
    parent.childActorOf(props)

    Range(0, n/batch).foreach { i =>
      parent.expectMsg(BatchComplete(group, (i+1).toLong * batch))
    }
    parent.expectMsg(GroupComplete(group))
    director.updates.get(update.source.id).subsetOf(
      devs.toSet
    ) shouldBe true
  }

  "group scheduler" should "set devices to `scheduled` when campaign is not set to auto-accept" in {
    val campaign = buildCampaignWithUpdate.copy(autoAccept = false)
    val update = updateRepo.findById(campaign.updateId).futureValue
    val group    = GroupId.generate()
    val n        = Gen.choose(1, batch-1).sample.get
    val devs     = Gen.listOfN(n, genDeviceId).sample.get

    clearClientState()

    deviceRegistry.setGroup(group, devs)
    campaigns.create(campaign, Set(group), Seq.empty).futureValue
    director.affected.put(update.source.id, devs.toSet)

    val parent = TestProbe()
    val props  = GroupScheduler.props(deviceRegistry, director, schedulerDelay, schedulerBatchSize, campaign, group)
    parent.childActorOf(props)

    parent.expectMsg(GroupComplete(group))

    val deviceStatus = deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.scheduled).futureValue

    deviceStatus shouldNot be(empty)
    deviceStatus shouldBe devs.toSet

    director.updates.asScala.get(update.source.id) shouldBe empty
  }

  "PRO-3745: group scheduler" should "properly set devices to `accepted` when affected devices < batch size" in {
    val campaign = buildCampaignWithUpdate
    val update = updateRepo.findById(campaign.updateId).futureValue
    val group    = GroupId.generate()
    val n        = Gen.choose(1, batch-1).sample.get
    val devs     = Gen.listOfN(n, genDeviceId).sample.get

    campaigns.create(campaign, Set(group), Seq.empty).futureValue

    clearClientState()
    deviceRegistry.setGroup(group, devs)

    val parent = TestProbe()
    val props  = GroupScheduler.props(deviceRegistry, director, schedulerDelay, schedulerBatchSize, campaign, group)
    parent.childActorOf(props)

    parent.expectMsg(GroupComplete(group))

    val deviceStatus = deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.accepted).futureValue

    deviceStatus shouldNot be(empty)
    deviceStatus shouldBe director.updates.get(update.source.id)
  }
}
