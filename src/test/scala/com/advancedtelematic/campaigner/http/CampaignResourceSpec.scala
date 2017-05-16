package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.util.{ResourceSpec, CampaignerSpec}
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

  property("POST /campaigns creates a campaign, GET /campaigns returns the created campaign") {
    forAll { request: CreateCampaign =>

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
  }

  property("PUT /campaigns/:campaign_id updates a campaign") {
    forAll { (request: CreateCampaign, update: UpdateCampaign) =>

      val id = createCampaignOk(request)
      val createdAt = getCampaignOk(id).createdAt

      Put(apiUri("campaigns/" + id.show), update).withHeaders(header) ~> routes ~> check {
        status shouldBe OK
      }

      getCampaignOk(id).updatedAt.isBefore(createdAt) shouldBe false
    }
  }

  property("POST /campaigns/:campaign_id/launch triggers an update") {
    forAll { request: CreateCampaign  =>

      val campaignId = createCampaignOk(request)

      Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
        status shouldBe OK
      }

      Get(apiUri(s"campaigns/${campaignId.show}/stats")).withHeaders(header) ~> routes ~> check {
        status shouldBe OK
        responseAs[CampaignStatsResult] shouldBe
          CampaignStatsResult(campaignId, request.groups.map(_ -> Stats(0, 0)).toMap)
      }
    }
  }

}
