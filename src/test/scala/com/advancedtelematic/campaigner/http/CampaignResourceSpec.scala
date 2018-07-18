package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes._
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{CampaignSupport, Campaigns}
import com.advancedtelematic.campaigner.util.{CampaignerSpec, ResourceSpec}
import com.advancedtelematic.libats.data.ErrorRepresentation
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Json
import io.circe.syntax._
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

class CampaignResourceSpec extends CampaignerSpec
    with ResourceSpec
    with CampaignSupport {

  val campaigns = Campaigns()

  def checkStats(
    id: CampaignId,
    campaignStatus: CampaignStatus,
    stats: Map[GroupId, Stats] = Map.empty,
    finished: Long = 0,
    failed: Set[DeviceId] = Set.empty,
    cancelled: Long = 0): Unit =
    Get(apiUri(s"campaigns/${id.show}/stats")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      responseAs[CampaignStats] shouldBe CampaignStats(id, campaignStatus, finished, failed, cancelled, stats)
    }

  "POST and GET /campaigns" should "create a campaign, return the created campaign" in {
    val request = arbitrary[CreateCampaign].sample.get
    val id = createCampaignOk(request)

    val campaign = getCampaignOk(id)
    campaign shouldBe GetCampaign(
      testNs,
      id,
      request.name,
      request.update,
      campaign.createdAt,
      campaign.updatedAt,
      request.groups,
      request.metadata.toList.flatten
    )
    campaign.createdAt shouldBe campaign.updatedAt

    val campaigns = getCampaignsOk()
    campaigns.values should contain (id)

    checkStats(id, CampaignStatus.prepared)
  }

  "PUT /campaigns/:campaign_id" should "update a campaign" in {
    val request = arbitrary[CreateCampaign].sample.get
    val update  = arbitrary[UpdateCampaign].sample.get

    val id = createCampaignOk(request)
    val createdAt = getCampaignOk(id).createdAt

    Put(apiUri("campaigns/" + id.show), update).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    val updated = getCampaignOk(id)

    updated.updatedAt.isBefore(createdAt) shouldBe false
    updated.name shouldBe update.name
    updated.metadata shouldBe update.metadata.get

    checkStats(id, CampaignStatus.prepared)
  }

  "POST /campaigns/:campaign_id/launch" should "trigger an update" in {
    val campaign = arbitrary[CreateCampaign].sample.get
    val campaignId = createCampaignOk(campaign)
    val request = Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header)

    request ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.scheduled,
      campaign.groups.map(_ -> Stats(0, 0)).toMap)

    request ~> routes ~> check {
      status shouldBe Conflict
    }
  }

  "POST /campaigns/:campaign_id/cancel" should "cancel a campaign" in {
    val campaign = arbitrary[CreateCampaign].sample.get
    val campaignId = createCampaignOk(campaign)

    checkStats(campaignId, CampaignStatus.prepared)

    Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.scheduled,
      campaign.groups.map(_ -> Stats(0, 0)).toMap)

    Post(apiUri(s"campaigns/${campaignId.show}/cancel")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.finished,
      campaign.groups.map(_ -> Stats(0, 0)).toMap)
  }

  "POST /cancel_device_update_campaign" should "cancel a single device update" in {
    val campaign   = arbitrary[CreateCampaign].sample.get
    val campaignId = createCampaignOk(campaign)
    val update     = campaign.update
    val device     = DeviceId.generate()

    Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.scheduled,
      campaign.groups.map(_ -> Stats(0, 0)).toMap)

    campaigns.scheduleDevice(campaignId, update, device).futureValue

    val entity = Json.obj("update" -> update.asJson, "device" -> device.asJson)
    Post(apiUri("cancel_device_update_campaign"), entity).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      director.cancelled.containsKey(device) shouldBe true
    }

    checkStats(campaignId, CampaignStatus.scheduled,
      campaign.groups.map(_ -> Stats(0, 0)).toMap, 0, Set.empty, 1)
  }

  it should "fail if device has not been scheduled" in {
    val campaign   = arbitrary[CreateCampaign].sample.get
    val campaignId = createCampaignOk(campaign)
    val update     = campaign.update
    val device     = DeviceId.generate()

    Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.scheduled,
      campaign.groups.map(_ -> Stats(0, 0)).toMap)

    val entity = Json.obj("update" -> update.asJson, "device" -> device.asJson)
    Post(apiUri("cancel_device_update_campaign"), entity).withHeaders(header) ~> routes ~> check {
      status shouldBe PreconditionFailed
      director.cancelled.containsKey(device) shouldBe true
    }

    checkStats(campaignId, CampaignStatus.scheduled,
      campaign.groups.map(_ -> Stats(0, 0)).toMap)
  }

  it should "accept request to cancel device if no campaign is associated" in {
    val update = UpdateId.generate()
    val device = DeviceId.generate()

    val entity = Json.obj("update" -> update.asJson, "device" -> device.asJson)
    Post(apiUri("cancel_device_update_campaign"), entity).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      director.cancelled.containsKey(device) shouldBe true
    }
  }

  it should "accept metadata as part of campaign creation" in {
    val request = arbitrary[CreateCampaign].retryUntil(_.metadata.nonEmpty).sample.get
    createCampaignOk(request)
  }

  it should "return metadata if campaign has it" in {
    val request = arbitrary[CreateCampaign].retryUntil(_.metadata.nonEmpty).sample.get
    val id = createCampaignOk(request)

    val result = getCampaignOk(id)

    result.metadata shouldBe request.metadata.get
  }

  it should "return error when metadata already exists" in {
    val metadata = Gen.listOfN(2, genCampaignMetadata).sample.get
    val request = arbitrary[CreateCampaign].sample.get.copy(metadata = Some(metadata))

    Post(apiUri("campaigns"), request).withHeaders(header) ~> routes ~> check {
      status shouldBe Conflict
      responseAs[ErrorRepresentation].code shouldBe ErrorCodes.ConflictingMetadata
    }
  }
}
