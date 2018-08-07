package com.advancedtelematic.campaigner.daemon

import java.time.Instant

import akka.Done
import com.advancedtelematic.campaigner.client.FakeDirectorClient
import com.advancedtelematic.campaigner.daemon.DeviceEventListener.AcceptedCampaign
import com.advancedtelematic.campaigner.data.DataType.{Campaign, DeviceStatus}
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, DeviceUpdateSupport}
import com.advancedtelematic.campaigner.util.CampaignerSpec
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, Event}
import com.advancedtelematic.libats.messaging_datatype.Messages
import com.advancedtelematic.libats.messaging_datatype.Messages.DeviceEventMessage
import com.advancedtelematic.libats.test.DatabaseSpec
import io.circe.syntax._
import org.scalacheck.Arbitrary._

import scala.concurrent.ExecutionContext.Implicits.global

class DeviceEventListenerSpec extends CampaignerSpec with DatabaseSpec with DeviceUpdateSupport {
  lazy val director = new FakeDirectorClient()

  val listener = new DeviceEventListener(director)

  val campaigns = Campaigns()

  def genDeviceEvent(campaign: Campaign, deviceId: DeviceId): DeviceEventMessage = {
    val payload = AcceptedCampaign(campaign.id)
    val event = Event(deviceId, "", DeviceEventListener.CampaignAcceptedEventType, Instant.now, Instant.now, payload.asJson)
    Messages.DeviceEventMessage(campaign.namespace, event)
  }

  "listener" should "schedule update in director" in {
    val campaign = arbitrary[Campaign].generate
    val device = arbitrary[DeviceId].generate
    val msg = genDeviceEvent(campaign, device)

    campaigns.create(campaign, Set.empty, Seq.empty).futureValue

    listener.apply(msg).futureValue shouldBe Done

    director.updates.get(campaign.updateId) shouldBe Set(device)
  }

  it should "set device update status to accepted" in {
    val campaign = arbitrary[Campaign].generate
    val device = arbitrary[DeviceId].generate
    val msg = genDeviceEvent(campaign, device)

    campaigns.create(campaign, Set.empty, Seq.empty).futureValue

    listener.apply(msg).futureValue shouldBe Done

    deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.accepted).futureValue shouldBe Set(device)
  }

  it should "set device to failed if device is no longer affected" in {
    val campaign = arbitrary[Campaign].generate
    val device = arbitrary[DeviceId].generate
    val msg = genDeviceEvent(campaign, device)

    campaigns.create(campaign, Set.empty, Seq.empty).futureValue

    director.cancelled.add(device)

    listener.apply(msg).futureValue shouldBe Done

    deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.failed).futureValue shouldBe Set(device)
  }
}
