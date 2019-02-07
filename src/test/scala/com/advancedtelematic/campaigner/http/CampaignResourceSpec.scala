package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes._
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{CampaignSupport, Campaigns}
import com.advancedtelematic.campaigner.util.{CampaignerSpec, ResourceSpec, UpdateResourceSpecUtil}
import com.advancedtelematic.libats.data.ErrorCodes.InvalidEntity
import com.advancedtelematic.libats.data.ErrorRepresentation
import com.advancedtelematic.libats.data.DataType.{CorrelationId, CampaignId => CampaignCorrelationId, MultiTargetUpdateId}
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Json
import io.circe.parser.parse
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
      request.parentCampaignId,
      request.groups.toList.toSet,
      request.metadata.toList.flatten,
      autoAccept = true
    )
    campaign.createdAt shouldBe campaign.updatedAt

    val campaigns = getCampaignsOk()
    campaigns.values should contain (id)

    checkStats(id, CampaignStatus.prepared)
  }

  "POST /campaigns" should "set parent_campaign_id if a valid one is given" in {
    val (parentId, _) = createCampaignWithUpdateOk()
    val (childId, request) = createCampaignWithUpdateOk(
      genCreateCampaign().map(_.copy(parentCampaignId = Some(parentId))))

    val campaign = getCampaignOk(childId)
    campaign shouldBe GetCampaign(
      testNs,
      childId,
      request.name,
      request.update,
      CampaignStatus.prepared,
      campaign.createdAt,
      campaign.updatedAt,
      Some(parentId),
      request.groups.toList.toSet,
      request.metadata.toList.flatten,
      autoAccept = true
    )
    campaign.createdAt shouldBe campaign.updatedAt

    val campaigns = getCampaignsOk()
    campaigns.values should contain (childId)

    checkStats(childId, CampaignStatus.prepared)
  }

  "POST /campaigns" should "fail if parent_campaign_id does not refer to an existing campaign" in {
      Given("a valid update")
      val createUpdate = genCreateUpdate().map(cu =>
          cu.copy(updateSource = UpdateSource(cu.updateSource.id, UpdateType.multi_target))
      ).sample.get
      val updateId = createUpdateOk(createUpdate)

      Given("a non-existing parent campaign ID")
      val genCreateCampaignWithInvalidParentId = for {
        createCampaign <- genCreateCampaign()
        parentId <- arbitrary[CampaignId]
      } yield createCampaign.copy(parentCampaignId = Some(parentId), update = updateId)
      val createCampaignWithInvalidParentId = genCreateCampaignWithInvalidParentId.sample.get

      When("a child campaign is created")
      Then("a PreconditionFailed error is raised")
      createCampaign(createCampaignWithInvalidParentId) ~> routes ~> check {
        // TODO: write a more proper check once
        // https://github.com/advancedtelematic/libats/pull/91 is merged
        status shouldBe PreconditionFailed
      }
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
      request.parentCampaignId,
      request.groups.toList.toSet,
      request.metadata.toList.flatten,
      autoAccept = true
    )
  }

  "POST /campaigns without groups" should "fail with InvalidEntity" in {
    val request = parse(
      """{
             "name" : "A campaign without groups",
             "update" : "84ab8e3d-6667-4edb-83ad-6cffbe064801",
             "groups" : []
             }""").valueOr(throw _)
    createCampaign(request) ~> routes ~> check {
      status shouldBe BadRequest
      responseAs[ErrorRepresentation].code shouldBe InvalidEntity
    }
  }

  "PUT /campaigns/:campaign_id" should "update a campaign" in {
    val (id, request) = createCampaignWithUpdateOk()
    val update  = arbitrary[UpdateCampaign].generate
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

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toList.toMap)

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

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toList.toMap)

    Post(apiUri(s"campaigns/${campaignId.show}/cancel")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.cancelled, campaign.groups.map(_ -> Stats(0, 0)).toList.toMap)
  }

  "POST /cancel_device_update_campaign" should "cancel a single device update" in {
    val (campaignId, campaign) = createCampaignWithUpdateOk()
    val updateId = campaign.update
    val device = DeviceId.generate()

    Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toList.toMap)

    campaigns.scheduleDevices(campaignId, updateId, device).futureValue

    val correlationId: CorrelationId = MultiTargetUpdateId(updateId.uuid)
    val entity = Json.obj("correlationId" -> correlationId.asJson, "device" -> device.asJson)
    Post(apiUri("cancel_device_update_campaign"), entity).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      fakeDirector.cancelled.contains(device) shouldBe true
    }

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toList.toMap, 0, Set.empty, 1)
  }

  it should "cancel a single device by campaign correlation id" in {
    val (campaignId, campaign) = createCampaignWithUpdateOk()
    val device = DeviceId.generate()

    Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toList.toMap)

    val correlationId: CorrelationId = CampaignCorrelationId(campaignId.uuid)
    val entity = Json.obj("correlationId" -> correlationId.asJson, "device" -> device.asJson)
    Post(apiUri("cancel_device_update_campaign"), entity).withHeaders(header) ~> routes ~> check {
      status shouldBe PreconditionFailed
      fakeDirector.cancelled.contains(device) shouldBe true
    }

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toList.toMap)
  }

  it should "fail if device has not been scheduled" in {
    val (campaignId, campaign) = createCampaignWithUpdateOk()
    val updateId = campaign.update
    val device     = DeviceId.generate()

    Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toList.toMap)

    val correlationId: CorrelationId = MultiTargetUpdateId(updateId.uuid)
    val entity = Json.obj("correlationId" -> correlationId.asJson, "device" -> device.asJson)
    Post(apiUri("cancel_device_update_campaign"), entity).withHeaders(header) ~> routes ~> check {
      status shouldBe PreconditionFailed
      fakeDirector.cancelled.contains(device) shouldBe true
    }

    checkStats(campaignId, CampaignStatus.launched, campaign.groups.map(_ -> Stats(0, 0)).toList.toMap)
  }

  it should "accept request to cancel device if no campaign is associated" in {
    val update = UpdateId.generate()
    val device = DeviceId.generate()

    val correlationId: CorrelationId = MultiTargetUpdateId(update.uuid)
    val entity = Json.obj("correlationId" -> correlationId.asJson, "device" -> device.asJson)
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
    val updateId = createUpdateOk(arbitrary[CreateUpdate].generate)
    val metadata = Seq(CreateCampaignMetadata(MetadataType.DESCRIPTION, "desc"), CreateCampaignMetadata(MetadataType.DESCRIPTION, "desc 2"))
    val createRequest = arbitrary[CreateCampaign].map(_.copy(update = updateId, metadata = Some(metadata))).generate

    Post(apiUri("campaigns"), createRequest).withHeaders(header) ~> routes ~> check {
      status shouldBe Conflict
      responseAs[ErrorRepresentation].code shouldBe ErrorCodes.ConflictingMetadata
    }
  }

  "GET all campaigns" should "return only campaigns matching campaign filter" in {
    val request = arbitrary[CreateCampaign].generate
    val (id, _) = createCampaignWithUpdateOk(request)

    getCampaignsOk(CampaignStatus.prepared.some).values should contain(id)

    getCampaignsOk(CampaignStatus.launched.some).values shouldNot contain(id)
  }

  "GET all campaigns" should "get all campaigns sorted by name when no sorting is given" in {
    val requests = Gen.listOfN(10, genCreateCampaign(Gen.alphaNumStr.retryUntil(_.nonEmpty))).generate
    val sortedNames = requests.map(_.name).sortBy(_.toLowerCase)
    requests.map(createCampaignWithUpdateOk(_))

    val campaignNames = getCampaignsOk().values.map(getCampaignOk).map(_.name).filter(sortedNames.contains)
    campaignNames shouldBe sortedNames
  }

  "GET all campaigns sorted by creation time" should "sort the campaigns from newest to oldest" in {
    val requests = Gen.listOfN(10, genCreateCampaign()).generate
    requests.map(createCampaignWithUpdateOk(_))

    val campaignsNewestToOldest = getCampaignsOk(sortBy = Some(SortBy.CreatedAt)).values.map(getCampaignOk)
    campaignsNewestToOldest.reverse.map(_.createdAt) shouldBe sorted
  }

  "GET campaigns filtered by name" should "get only the campaigns that contain the filter parameter" in {
    val names = Seq("aabb", "baaxbc", "a123ba", "cba3b")
    val campaignsIdNames = names
      .map(Gen.const)
      .map(genCreateCampaign)
      .map(createCampaignWithUpdateOk)
      .map(_._1)
      .map(getCampaignOk)
      .map(c => c.id -> c.name)
      .toMap

    val tests = Map("" -> names, "a1" -> Seq("a123ba"), "aa" -> Seq("aabb", "baaxbc"), "3b" -> Seq("a123ba", "cba3b"), "3" -> Seq("a123ba", "cba3b"))

    tests.foreach{ case (nameContains, expected) =>
      val resultIds = getCampaignsOk(nameContains = Some(nameContains)).values.filter(campaignsIdNames.keySet.contains)
      val resultNames = campaignsIdNames.filterKeys(resultIds.contains).values
      resultNames.size shouldBe expected.size
      resultNames should contain allElementsOf expected
    }
  }

}
