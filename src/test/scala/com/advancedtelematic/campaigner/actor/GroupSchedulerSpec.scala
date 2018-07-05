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
import scala.concurrent.Future

class GroupSchedulerSpec extends ActorSpec[GroupScheduler] with CampaignerSpec {

  import Arbitrary._
  import GroupScheduler._
  import scala.concurrent.duration._

  def clearClientState() = {
    registry.state.clear()
    director.updates.clear()
    director.cancelled.clear()
  }

  def fakeRegistry(devs: Seq[DeviceId]) = new DeviceRegistryClient {
    override def devicesInGroup(ns: Namespace, grp: GroupId, offset: Long, limit: Long): Future[Seq[DeviceId]] =
      FastFuture.successful(devs.drop(offset.toInt).take(limit.toInt))
  }

  val campaigns = Campaigns()

  "group scheduler" should "trigger updates for each device in batch" in {
    val campaign = arbitrary[Campaign].sample.get
    val group    = GroupId.generate()
    val parent = TestProbe()
    val props  = GroupScheduler.props(registry, director, 10.minutes, schedulerBatchSize, campaign, group)

    clearClientState()
    campaigns.create(campaign, Set(group)).futureValue

    parent.childActorOf(props)
    parent.expectMsgPF(10.seconds) {
      case BatchComplete(grp, _) => grp
      case GroupComplete(grp)    => grp
    }
    registry.state.get(group).take(batch).toSet should contain allElementsOf (
      director.updates.get(campaign.updateId).toSet
    )
  }

  "group scheduler" should "respect groups with processed devices > batch size" in {
    val campaign = arbitrary[Campaign].sample.get
    val group    = GroupId.generate()
    val n        = Gen.choose(batch, batch * 10).sample.get
    val devs     = Gen.listOfN(n, genDeviceId).sample.get

    campaigns.create(campaign, Set(group)).futureValue

    clearClientState()

    val parent = TestProbe()
    val props  = GroupScheduler.props(fakeRegistry(devs), director, schedulerDelay, schedulerBatchSize, campaign, group)
    parent.childActorOf(props)

    Range(0, n/batch).foreach { i =>
      parent.expectMsg(BatchComplete(group, (i+1).toLong * batch))
    }
    parent.expectMsg(GroupComplete(group))
    director.updates.get(campaign.updateId).subsetOf(
      devs.toSet
    ) shouldBe true
  }

  "PRO-3745: group scheduler" should "properly set devices to `scheduled` when affected devices < batch size" in {
    val campaign = arbitrary[Campaign].sample.get
    val group    = GroupId.generate()
    val n        = Gen.choose(0, batch-1).sample.get
    val devs     = Gen.listOfN(n, genDeviceId).sample.get

    campaigns.create(campaign, Set(group)).futureValue

    clearClientState()

    val parent = TestProbe()
    val props  = GroupScheduler.props(fakeRegistry(devs), director, schedulerDelay, schedulerBatchSize, campaign, group)
    parent.childActorOf(props)

    parent.expectMsg(GroupComplete(group))

    director.updates.get(campaign.updateId) should be
      campaigns.scheduledDevices(campaign.namespace, campaign.id).futureValue
  }
}
