package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.util.{CampaignerSpec, ResourceSpec}
import com.advancedtelematic.libats.data.Namespace
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import org.scalacheck.Arbitrary._

class CampaignResourceSpec extends CampaignerSpec with ResourceSpec {

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
  }

  "POST /campaigns/:campaign_id/launch" should "trigger an update" in {
    val campaign = arbitrary[CreateCampaign].sample.get
    val campaignId = createCampaignOk(campaign)
    val request = Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header)

    request ~> routes ~> check {
      status shouldBe OK
    }

    Get(apiUri(s"campaigns/${campaignId.show}/stats")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      responseAs[CampaignStats] shouldBe CampaignStats(
        campaignId,
        CampaignStatus.scheduled,
        0,
        Set.empty,
        campaign.groups.map(_ -> Stats(0, 0)).toMap
      )
    }

    request ~> routes ~> check {
      status shouldBe Conflict
    }
  }

  "POST /campaigns/:campaign_id/cancel" should "cancel a campaign" in {
    val campaign = arbitrary[CreateCampaign].sample.get
    val campaignId = createCampaignOk(campaign)

    Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    Post(apiUri(s"campaigns/${campaignId.show}/cancel")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

  }

  def checkStatus(id: CampaignId, expected: CampaignStatus) =
    Get(apiUri(s"campaigns/${id.show}/stats")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      val result = responseAs[CampaignStats]
      result.campaign shouldBe id
      result.status shouldBe expected
    }

  "campaign resource" should "undergo proper status transitions" in {
    val campaign = arbitrary[CreateCampaign].sample.get
    val id = createCampaignOk(campaign)

    checkStatus(id, CampaignStatus.prepared)

    Post(apiUri(s"campaigns/${id.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStatus(id, CampaignStatus.scheduled)

    Post(apiUri(s"campaigns/${id.show}/cancel")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStatus(id, CampaignStatus.cancelled)
  }

}
