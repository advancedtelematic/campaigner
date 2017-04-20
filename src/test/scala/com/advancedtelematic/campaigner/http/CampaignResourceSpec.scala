package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.util.{ResourceSpec, CampaignerSpec}
import de.heikoseeberger.akkahttpcirce.CirceSupport._
import org.scalacheck.Arbitrary._
import scala.collection.JavaConverters._

class CampaignResourceSpec extends CampaignerSpec with ResourceSpec {

  def createCampaignOk(request: CreateCampaign): CampaignId =
    Post(apiUri("campaigns"), request) ~> routes ~> check {
      status shouldBe StatusCodes.Created
      responseAs[CampaignId]
    }

  def getCampaignOk(id: CampaignId): GetCampaign =
    Get(apiUri("campaigns/" + id.show)) ~> routes ~> check {
      status shouldBe StatusCodes.OK
      responseAs[GetCampaign]
    }

  property("POST /campaigns creates a campaign, GET /campaigns returns the created campaign") {
    forAll { request: CreateCampaign =>

      val id = createCampaignOk(request)

      val campaign = getCampaignOk(id)
      campaign shouldBe GetCampaign(
        id,
        request.namespace,
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

      Put(apiUri("campaigns/" + id.show), update) ~> routes ~> check {
        status shouldBe StatusCodes.OK
      }

      getCampaignOk(id).updatedAt.isBefore(createdAt) shouldBe false
    }
  }

  property("POST /campaigns/:campaign_id/launch triggers an update") {
    forAll { request: CreateCampaign  =>

      val campaignId = createCampaignOk(request)

      Post(apiUri(s"campaigns/${campaignId.show}/launch")) ~> routes ~> check {
        status shouldBe StatusCodes.OK
      }
    }
  }

  property("all affected groups and devices get updates") {
    forAll { request: CreateCampaign  =>

      val campaignId = createCampaignOk(request)

      director.updatedDevices.clear()
      Post(apiUri(s"campaigns/${campaignId.show}/launch")) ~> routes ~> check {
        status shouldBe StatusCodes.OK
        request.groups shouldBe deviceRegistry.pseudoState.keys.asScala.toSet.intersect(request.groups)
        request.groups.map { grp =>
          deviceRegistry.pseudoState.get(grp)
        }.flatten.toSet shouldBe director.updatedDevices.keys.asScala.toSet
      }
    }
  }

}
