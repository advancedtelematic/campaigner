package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes
import com.advancedtelematic.campaigner.data.DataType.{GetDeviceCampaigns, CreateCampaign}
import com.advancedtelematic.campaigner.db.{CampaignSupport, Campaigns}
import com.advancedtelematic.campaigner.util.{CampaignerSpec, ResourceSpec}
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import org.scalacheck.Arbitrary._
import com.advancedtelematic.campaigner.data.Generators._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import com.advancedtelematic.campaigner.data.Codecs._

class DeviceResourceSpec
  extends CampaignerSpec
  with ResourceSpec
  with CampaignSupport {

  val campaigns = Campaigns()

  it should "returns scheduled campaigns for device" in {
    val request = arbitrary[CreateCampaign].retryUntil(_.metadata.nonEmpty).sample.get
    val campaignId = createCampaignOk(request)
    val device = DeviceId.generate()

    campaigns.scheduleDevice(campaignId, request.update, device).futureValue

    Get(apiUri(s"device/${device.uuid}/campaigns")).withHeaders(header) ~> routes ~> check {
      status shouldBe StatusCodes.OK

      val res = responseAs[GetDeviceCampaigns]

      res.deviceId shouldBe device
      res.campaigns.map(_.id) shouldBe Seq(campaignId)
      res.campaigns.flatMap(_.metadata) shouldBe request.metadata
    }
  }
}
