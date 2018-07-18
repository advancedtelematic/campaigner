package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, CampaignSupport}
import com.advancedtelematic.campaigner.util.{CampaignerSpec, ResourceSpec}
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Json
import io.circe.syntax._
import org.scalacheck.Arbitrary._

// TODO:SM update sbt

class CampaignResourceSpec extends CampaignerSpec
    with ResourceSpec
    with CampaignSupport {

  val campaigns = Campaigns()

  def testNs = Namespace("testNs")
  def header = RawHeader("x-ats-namespace", testNs.get)

  def createCampaignOk(request: CreateCampaign): CampaignId =
    Post(apiUri("campaigns"), request).withHeaders(header) ~> routes ~> check {
      status shouldBe Created
      responseAs[CampaignId]
    }

  def getCampaignOk(id: CampaignId): GetCampaign =
    Get(apiUri("campaigns/" + id.show)).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      responseAs[GetCampaign]
    }

  def getCampaignsOk(): PaginationResult[CampaignId] =
    Get(apiUri("campaigns")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      responseAs[PaginationResult[CampaignId]]
    }

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
      request.groups
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

    getCampaignOk(id).updatedAt.isBefore(createdAt) shouldBe false

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

  it should "accept metadata as part of campaign creation" in pending

  it should "return metadata if campaign has it" in pending
}
