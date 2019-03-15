package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes._
import cats.data.NonEmptyList
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{CampaignSupport, Campaigns, DeviceUpdateSupport}
import com.advancedtelematic.campaigner.util.{CampaignerSpec, ResourceSpec, UpdateResourceSpecUtil}
import com.advancedtelematic.libats.data.ErrorCodes.InvalidEntity
import com.advancedtelematic.libats.data.ErrorRepresentation
import com.advancedtelematic.libats.data.DataType.{CorrelationId, MultiTargetUpdateId}
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalactic.source
import org.scalatest._
import org.scalatest.prop.PropertyChecks

import scala.concurrent.Future

class CampaignResourceSpec
    extends CampaignerSpec
    with ResourceSpec
    with CampaignSupport
    with DeviceUpdateSupport
    with UpdateResourceSpecUtil
    with GivenWhenThen
    with PropertyChecks {

  val campaigns = Campaigns()

  def checkStats(id: CampaignId, campaignStatus: CampaignStatus, processed: Long = 0, affected: Long = 0,
                 finished: Long = 0, failed: Set[DeviceId] = Set.empty, cancelled: Long = 0)
                (implicit pos: source.Position): Unit =
    Get(apiUri(s"campaigns/${id.show}/stats")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      val campaignStats = responseAs[CampaignStats]
      campaignStats.status shouldBe campaignStatus
      campaignStats shouldBe CampaignStats(id, campaignStatus, finished, failed, cancelled, processed, affected)
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
      request.mainCampaignId,
      Set.empty,
      request.groups.toList.toSet,
      request.metadata.toList.flatten,
      autoAccept = true
    )
    campaign.createdAt shouldBe campaign.updatedAt

    val campaigns = getCampaignsOk()
    campaigns.values should contain (id)

    checkStats(id, CampaignStatus.prepared)
  }

  "POST /campaigns" should "set mainCampaignId if a valid one is given" in {
    Given("a valid main campaign ID")
    val (mainId, _) = createCampaignWithUpdateOk()

    When("a retry campaign is created")
    val (retryId, request) = createCampaignWithUpdateOk(
      genCreateCampaign().map(_.copy(mainCampaignId = Some(mainId))))

    Then("the retry campaign should have the main campaign ID and other properties")
    val retryCampaign = getCampaignOk(retryId)
    retryCampaign shouldBe GetCampaign(
      testNs,
      retryId,
      request.name,
      request.update,
      CampaignStatus.prepared,
      retryCampaign.createdAt,
      retryCampaign.updatedAt,
      Some(mainId),
      Set.empty,
      request.groups.toList.toSet,
      request.metadata.toList.flatten,
      autoAccept = true
    )
    retryCampaign.createdAt shouldBe retryCampaign.updatedAt

    And("the list of all campaigns should NOT list the retry campaign")
    val campaigns = getCampaignsOk()
    campaigns.values should not contain retryId

    And("the retry campaign should have `prepared` status")
    checkStats(retryId, CampaignStatus.prepared)

    And("the main campaign should have the retry campaign in retry campaigns list")
    val mainCampaign = getCampaignOk(mainId)
    mainCampaign.retryCampaignIds should contain(retryId)
  }

  "POST /campaigns" should "fail if mainCampaignId does not refer to an existing campaign" in {
      Given("a valid update")
      val createUpdate = genCreateUpdate().map(cu =>
          cu.copy(updateSource = UpdateSource(cu.updateSource.id, UpdateType.multi_target))
      ).sample.get
      val updateId = createUpdateOk(createUpdate)

      Given("a non-existing main campaign ID")
      val genCreateCampaignWithInvalidMainId = for {
        createCampaign <- genCreateCampaign()
        mainId <- arbitrary[CampaignId]
      } yield createCampaign.copy(mainCampaignId = Some(mainId), update = updateId)
      val createCampaignWithInvalidMainId = genCreateCampaignWithInvalidMainId.sample.get

      When("a retry campaign is created")
      Then("a PreconditionFailed error is raised")
      createCampaign(createCampaignWithInvalidMainId) ~> routes ~> check {
        status shouldBe PreconditionFailed
        responseAs[ErrorRepresentation].code shouldBe Errors.MissingMainCampaign.code
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
      request.mainCampaignId,
      Set.empty,
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
    val (id, _) = createCampaignWithUpdateOk()
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

  "POST /campaigns/:campaign_id/retry-failed" should "create a retry-campaign" in {
    val (campaignId, campaign) = createCampaignWithUpdateOk()
    val deviceId = genDeviceId.generate
    val failureCode = "failureCode-1"
    val deviceUpdate = DeviceUpdate(campaignId, campaign.update, deviceId, DeviceStatus.accepted, Some(failureCode))

    deviceUpdateRepo.persistMany(deviceUpdate :: Nil).futureValue
    campaigns.failDevices(campaignId, deviceId :: Nil, failureCode).futureValue

    createRetryCampaign(campaignId, RetryFailedDevices(failureCode)) ~> routes ~> check {
      status shouldBe Created
    }
  }

  "POST /campaigns/:campaign_id/retry-failed" should "fail if there are no failed devices for the given failure code" in {
    val (campaignId, _) = createCampaignWithUpdateOk()
    val request = RetryFailedDevices("failureCode-2")

    createRetryCampaign(campaignId, request) ~> routes ~> check {
      status shouldBe PreconditionFailed
      responseAs[ErrorRepresentation].code shouldBe Errors.MissingFailedDevices(request.failureCode).code
    }
  }

  "POST /campaigns/:campaign_id/retry-failed" should "fail if there is no main campaign with id :campaign_id" in {
    val request = RetryFailedDevices("failureCode-3")
    createRetryCampaign(CampaignId.generate(), request) ~> routes ~> check {
      status shouldBe NotFound
      responseAs[ErrorRepresentation].code shouldBe Errors.CampaignMissing.code
    }
  }

  "POST /campaigns/:campaign_id/launch" should "trigger an update" in {
    val (campaignId, _) = createCampaignWithUpdateOk()
    val request = Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header)

    request ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.launched)

    request ~> routes ~> check {
      status shouldBe Conflict
    }
  }

  "POST /campaigns/:campaign_id/cancel" should "cancel a campaign" in {
    val (campaignId, _) = createCampaignWithUpdateOk()

    checkStats(campaignId, CampaignStatus.prepared)

    Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.launched)

    Post(apiUri(s"campaigns/${campaignId.show}/cancel")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    checkStats(campaignId, CampaignStatus.cancelled)
  }

  "POST /cancel_device_update_campaign" should "cancel a single device update" in {
    val (campaignId, campaign) = createCampaignWithUpdateOk()
    val updateId = campaign.update
    val device = DeviceId.generate()

    Post(apiUri(s"campaigns/${campaignId.show}/launch")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
    }

    campaigns.scheduleDevices(campaignId, updateId, device).futureValue

    val correlationId: CorrelationId = MultiTargetUpdateId(updateId.uuid)
    val entity = Json.obj("correlationId" -> correlationId.asJson, "device" -> device.asJson)
    Post(apiUri("cancel_device_update_campaign"), entity).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      fakeDirector.cancelled.contains(device) shouldBe true
    }
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

  "GET /campaigns/:id/stats" should "return correct statistics" in {
    val campaigns = Campaigns()

    case class CampaignCase(
        successfulDevices: Seq[DeviceId],
        failedDevices: Seq[DeviceId],
        cancelledDevices: Seq[DeviceId],
        notAffectedDevices: Seq[DeviceId]) {
      val groupId = genGroupId.generate
      val affectedDevices = successfulDevices ++ failedDevices ++ cancelledDevices
      val affectedCount = affectedDevices.size.toLong
      val notAffectedCount = notAffectedDevices.size.toLong
      val processedCount = affectedCount + notAffectedCount

      override def toString: String =
        s"CampaignCase(s=${successfulDevices.size}, f=${failedDevices.size}, c=${cancelledDevices.size}, a=$affectedCount, n=$notAffectedCount, p=$processedCount)"
    }

    def genCampaignCase: Gen[CampaignCase] = for {
      successfulDevices <- Gen.listOf(genDeviceId)
      failedDevices <- Gen.listOf(genDeviceId)
      cancelledDevices <- Gen.listOf(genDeviceId)
      notAffectedDevices <- Gen.listOf(genDeviceId)
    } yield CampaignCase(successfulDevices, failedDevices, cancelledDevices, notAffectedDevices)

    def conductCampaign(campaignId: CampaignId, campaign: CreateCampaign, campaignCase: CampaignCase): Future[Unit] = for {
      _ <- campaigns.launch(campaignId)
      _ <- campaigns.scheduleDevices(campaignId, campaign.update, campaignCase.affectedDevices:_*)
      _ <- campaigns.rejectDevices(campaignId, campaign.update, campaignCase.notAffectedDevices)
      _ <- campaigns.cancelDevices(campaignId, campaignCase.cancelledDevices)
      _ <- campaigns.failDevices(campaignId, campaignCase.failedDevices, "failure-code-1")
      _ <- campaigns.succeedDevices(campaignId, campaignCase.successfulDevices, "success-code-1")
    } yield ()

    forAll (genCampaignCase) (mainCase => {
      val (mainCampaignId, mainCampaign) = createCampaignWithUpdateOk(
        genCreateCampaign().map(_.copy(groups = NonEmptyList.one(mainCase.groupId)))
      )
      conductCampaign(mainCampaignId, mainCampaign, mainCase).futureValue

      Get(apiUri(s"campaigns/${mainCampaignId.show}/stats")).withHeaders(header) ~> routes ~> check {
        status shouldBe OK

        val campaignStats = responseAs[CampaignStats]
        campaignStats.status shouldBe CampaignStatus.finished
        campaignStats.finished shouldBe (mainCase.successfulDevices.size + mainCase.failedDevices.size)
        campaignStats.failed should contain theSameElementsAs mainCase.failedDevices
        campaignStats.cancelled shouldBe mainCase.cancelledDevices.size
        campaignStats.processed shouldBe mainCase.processedCount
        campaignStats.affected shouldBe mainCase.affectedCount
      }

      val Seq(retriedS, retriedF, retriedC, notRetriedDevices, notAffectedDevices) =
        splitIntoGroupsRandomly(mainCase.failedDevices, 5)
      val retryCase = CampaignCase(
        successfulDevices = retriedS,
        failedDevices = retriedF,
        cancelledDevices = retriedC,
        notAffectedDevices = notAffectedDevices
      )

      val (retryCampaignId, retryCampaign) = createCampaignWithUpdateOk(
        genCreateCampaign().map(_.copy(
          groups = NonEmptyList.one(retryCase.groupId),
          mainCampaignId = Some(mainCampaignId)
        )))
      conductCampaign(retryCampaignId, retryCampaign, retryCase).futureValue

      Get(apiUri(s"campaigns/${mainCampaignId.show}/stats")).withHeaders(header) ~> routes ~> check {
        status shouldBe OK

        val campaignStats = responseAs[CampaignStats]
        campaignStats.status shouldBe CampaignStatus.finished
        campaignStats.finished shouldBe (
          mainCase.successfulDevices.size + mainCase.failedDevices.size -
          (retryCase.processedCount - retryCase.affectedCount + retryCase.cancelledDevices.size)
        )
        campaignStats.cancelled shouldBe (mainCase.cancelledDevices.size + retryCase.cancelledDevices.size)
        campaignStats.processed shouldBe mainCase.processedCount
        campaignStats.affected shouldBe (
          mainCase.affectedCount -
          (retryCase.processedCount - retryCase.affectedCount)
        )
      }
    })
  }

  private def splitIntoGroupsRandomly[T](seq: Seq[T], n: Int): Seq[Seq[T]] = {
    val elemsWithGroupNumAssigned = seq.groupBy(_ => Gen.choose(0, n - 1).generate)
    0.until(n).map(groupNum => elemsWithGroupNumAssigned.getOrElse(groupNum, Seq.empty))
  }
}
