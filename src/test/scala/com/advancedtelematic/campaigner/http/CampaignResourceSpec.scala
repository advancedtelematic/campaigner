package com.advancedtelematic.campaigner.http


import akka.http.scaladsl.model.StatusCodes._
import cats.syntax.show._
import cats.syntax.option._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{CampaignSupport, Campaigns}
import com.advancedtelematic.campaigner.util.{CampaignerSpec, ResourceSpec, UpdateResourceSpecUtil}
import com.advancedtelematic.libats.data.ErrorRepresentation
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Json
import io.circe.syntax._
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalactic.source

class CampaignResourceSpec extends CampaignerSpec with ResourceSpec with CampaignSupport with UpdateResourceSpecUtil {

  val campaigns = Campaigns()

  def checkStats(id: CampaignId, campaignStatus: CampaignStatus, stats: Map[GroupId, Stats] = Map.empty,
                 finished: Long = 0, failed: Set[DeviceId] = Set.empty, cancelled: Long = 0)
                (implicit pos: source.Position): Unit =
    Get(apiUri(s"campaigns/${id.show}/stats")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      val campaignStats = responseAs[CampaignStats]
      campaignStats.status shouldBe campaignStatus
      campaignStats shouldBe CampaignStats(id, campaignStatus, finished, failed, cancelled, stats)
    }

  "POST and GET /campaigns" should "create a campaign, return the created campaign" in {
    val (id, request) = createCampaignWithUpdateOk()

    val campaign = getCampaignOk(id)
    campaign shouldBe GetCampaign(
      testNs,
      id,
      request.name,
      request.update,
      CampaignStatus.prepared,
      campaign.createdAt,
      campaign.updatedAt,
      request.groups,
      request.metadata.toList.flatten,
      autoAccept = true
    )
    campaign.createdAt shouldBe campaign.updatedAt

    val campaigns = getCampaignsOk()
    campaigns.values should contain (id)

    checkStats(id, CampaignStatus.prepared)
  }

  "POST/GET autoAccept campaign" should "create and return the created campaign" in {
    val (id, request) = createCampaignWithUpdateOk(arbitrary[CreateCampaign].map(_.copy(approvalNeeded = Some(false))))

    val campaign = getCampaignOk(id)

    campaign shouldBe GetCampaign(
      testNs,
      id,
      request.name,
      request.update,
      CampaignStatus.prepared,
      campaign.createdAt,
      campaign.updatedAt,
      request.groups,
      request.metadata.toList.flatten,
      autoAccept = true
    )
  }

  "PUT /campaigns/:campaign_id" should "update a campaign" in {
    val (id, request) = createCampaignWithUpdateOk()
    val update  = arbitrary[UpdateCampaign].sample.get
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
    val (campaignId, campaign) = createCampaignWithUpdateOk()
    val request = Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header)

    request ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toMap)

    request ~> routes ~> check {
      status shouldBe Conflict
    }
  }

  "POST /campaigns/:campaign_id/cancel" should "cancel a campaign" in {
    val (campaignId, campaign) = createCampaignWithUpdateOk()

    checkStats(campaignId, CampaignStatus.prepared)

    Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toMap)

    Post(apiUri(s"campaigns/${campaignId.show}/cancel")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.cancelled,
      campaign.groups.map(_ -> Stats(0, 0)).toMap)
  }

  "POST /cancel_device_update_campaign" should "cancel a single device update" in {
    val (campaignId, campaign) = createCampaignWithUpdateOk()
    val updateId = campaign.update
    val device = DeviceId.generate()

    Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toMap)

    campaigns.scheduleDevices(campaignId, updateId, device).futureValue

    val entity = Json.obj("update" -> updateId.asJson, "device" -> device.asJson)
    Post(apiUri("cancel_device_update_campaign"), entity).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      fakeDirector.cancelled.contains(device) shouldBe true
    }

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toMap, 0, Set.empty, 1)
  }

  it should "fail if device has not been scheduled" in {
    val (campaignId, campaign) = createCampaignWithUpdateOk()
    val updateId = campaign.update
    val device     = DeviceId.generate()

    Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toMap)

    val entity = Json.obj("update" -> updateId.asJson, "device" -> device.asJson)
    Post(apiUri("cancel_device_update_campaign"), entity).withHeaders(header) ~> routes ~> check {
      status shouldBe PreconditionFailed
      fakeDirector.cancelled.contains(device) shouldBe true
    }

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toMap)
  }

  it should "accept request to cancel device if no campaign is associated" in {
    val update = UpdateId.generate()
    val device = DeviceId.generate()

    val entity = Json.obj("update" -> update.asJson, "device" -> device.asJson)
    Post(apiUri("cancel_device_update_campaign"), entity).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      fakeDirector.cancelled.contains(device) shouldBe true
    }
  }

  it should "return metadata if campaign has it" in {
    val (id, request) = createCampaignWithUpdateOk(arbitrary[CreateCampaign].retryUntil(_.metadata.nonEmpty))
    val result = getCampaignOk(id)

    result.metadata shouldBe request.metadata.get
  }

  it should "return error when metadata is duplicated in request" in {
    val updateId = createUpdateOk(arbitrary[CreateUpdate].gen)
    val metadata = Seq(CreateCampaignMetadata(MetadataType.DESCRIPTION, "desc"), CreateCampaignMetadata(MetadataType.DESCRIPTION, "desc 2"))
    val createRequest = arbitrary[CreateCampaign].map(_.copy(update = updateId, metadata = Some(metadata))).gen

    Post(apiUri("campaigns"), createRequest).withHeaders(header) ~> routes ~> check {
      status shouldBe Conflict
      responseAs[ErrorRepresentation].code shouldBe ErrorCodes.ConflictingMetadata
    }
  }

  "GET all campaigns" should "return only campaigns matching campaign filter" in {
    val request = arbitrary[CreateCampaign].sample.get
    val (id, _) = createCampaignWithUpdateOk(request)

    getCampaignsOk(CampaignStatus.prepared.some).values should contain(id)

    getCampaignsOk(CampaignStatus.launched.some).values shouldNot contain(id)
  }

  "GET all campaigns" should "get all campaigns sorted by name when no sorting is given" in {
    val requests = Gen.listOfN(20, genCreateCampaignWithAlphanumericName).sample.get
    val sortedNames = requests.map(_.name).sortWith(_.toLowerCase < _.toLowerCase)
    requests.map(createCampaignWithUpdateOk(_))

    val campaignNames = getCampaignsOk().values.map(getCampaignOk).map(_.name).filter(sortedNames.contains)
    campaignNames shouldBe sortedNames
  }

  "GET all campaigns sorted by creation time" should "sort the campaigns from newest to oldest" in {
    val requests = Gen.listOfN(20, genCreateCampaign).sample.get
    requests.map(createCampaignWithUpdateOk(_))

    val campaignsNewestToOldest = getCampaignsOk(sortBy = Some(SortBy.CreatedAt)).values.map(getCampaignOk)
    campaignsNewestToOldest.reverse.map(_.createdAt) shouldBe sorted
  }

}
