package com.advancedtelematic.campaigner.actor

import java.time.Instant

import akka.testkit.TestProbe
import com.advancedtelematic.campaigner.daemon.DeviceUpdateEventListener
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, UpdateSupport}
import com.advancedtelematic.campaigner.util.{ActorSpec, CampaignerSpec, DatabaseUpdateSpecUtil, TestMessageBus}
import com.advancedtelematic.libats.data.DataType.{CampaignId => CampaignCorrelationId}
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import com.advancedtelematic.libats.messaging_datatype.Messages._
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

import scala.concurrent.Future

class CampaignSchedulerSpec extends ActorSpec[CampaignScheduler] with CampaignerSpec with UpdateSupport with DatabaseUpdateSpecUtil {
  import Arbitrary._
  import CampaignScheduler._

  import scala.concurrent.duration._

  val campaigns = Campaigns()

  def buildCampaignWithUpdate: Campaign = {
    val update = genMultiTargetUpdate.generate
    val updateId = updateRepo.persist(update).futureValue
    arbitrary[Campaign].generate.copy(updateId = updateId)
  }

  "campaign scheduler" should "trigger updates for each device" in {
    val campaign = buildCampaignWithUpdate
    val parent   = TestProbe()
    val n = Gen.choose(batch, batch * 2).generate
    val devices = Gen.listOfN(n, genDeviceId).generate.toSet

    implicit val msgBus = new TestMessageBus()

    campaigns.create(campaign, Set.empty, devices, Seq.empty).futureValue

    parent.childActorOf(CampaignScheduler.props(
      campaign,
      schedulerDelay,
      schedulerBatchSize
    ))
    parent.expectMsg(1.minute, CampaignComplete(campaign.id))

    val actualDevices = msgBus.messages.map {
      case msg: DeviceUpdateAssignmentRequested => msg.deviceUuid
    }.toSet

    actualDevices shouldBe devices
  }

  "PRO-3672: campaign with 0 affected devices" should "yield a `finished` status" in {
    val campaign = buildCampaignWithUpdate
    val parent   = TestProbe()
    val n = Gen.choose(batch, batch * 2).generate
    val devices = Gen.listOfN(n, genDeviceId).generate.toSet

    implicit val msgBus = new TestMessageBus()

    campaigns.create(campaign, Set.empty, devices, Seq.empty).futureValue

    parent.childActorOf(CampaignScheduler.props(
      campaign,
      schedulerDelay,
      schedulerBatchSize
    ))
    parent.expectMsg(20.seconds, CampaignComplete(campaign.id))

    val actualDevices = msgBus.messages.map {
      case msg: DeviceUpdateAssignmentRequested => msg.deviceUuid
    }

    emulateRejectionByDirector(campaign, actualDevices).futureValue

    campaigns.campaignStats(campaign.id).futureValue.status shouldBe CampaignStatus.finished
  }

  private def emulateRejectionByDirector(campaign: Campaign, devices: Seq[DeviceId]): Future[Unit] = {
    val listener = new DeviceUpdateEventListener()
    Future.traverse(devices) { deviceId =>
      val msg = DeviceUpdateAssignmentRejected(
        campaign.namespace,
        Instant.now(),
        CampaignCorrelationId(campaign.id.uuid),
        deviceId)
      listener(msg)
    }.map(_ => ())
  }
}
