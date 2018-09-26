package com.advancedtelematic.campaigner.http

import java.util.UUID

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.Location
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.GroupId._
import com.advancedtelematic.campaigner.data.DataType.SortBy.SortBy
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.UpdateSupport
import com.advancedtelematic.campaigner.util.{CampaignerSpec, ResourceSpec}
import com.advancedtelematic.libats.data.{ErrorRepresentation, PaginationResult}
import com.advancedtelematic.libats.messaging_datatype.DataType.UpdateId
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

class UpdateResourceSpec extends CampaignerSpec with ResourceSpec with UpdateSupport {

  private def createUpdate(request: CreateUpdate): HttpRequest =
    Post(apiUri("updates"), request).withHeaders(header)

  private def createUpdateOk(request: CreateUpdate): UpdateId =
    createUpdate(request) ~> routes ~> check {
      status shouldBe Created
      import org.scalatest.OptionValues._
      header[Location].value.uri.isAbsolute shouldBe true
      responseAs[UpdateId]
    }

  private def getUpdates(groupId: Option[GroupId] = None, nameContains: Option[String] = None): HttpRequest = {
    val m = List("groupId" -> groupId, "nameContains" -> nameContains).collect{
      case (k, Some(v: GroupId)) => k -> v.show
      case (k, Some(v)) => k -> v.toString
    }
    Get(apiUri("updates").withQuery(Query(m.toMap))).withHeaders(header)
  }

  private def getUpdateOk(updateId: UpdateId): Update =
    Get(apiUri(s"updates/${updateId.show}")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      responseAs[Update]
    }

  private def getUpdatesOk(nameContains: String): PaginationResult[Update] =
    Get(apiUri("updates").withQuery(Query("nameContains" -> nameContains))).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      responseAs[PaginationResult[Update]]
    }

  private def getUpdatesSorted(sortBy: SortBy): HttpRequest =
    Get(apiUri("updates").withQuery(Query("sortBy" -> sortBy.toString))).withHeaders(header)

  private def getUpdateResult(id: UpdateId): RouteTestResult = {
    Get(apiUri(s"updates/${id.uuid.toString}")).withHeaders(header) ~> routes
  }

  "GET to /updates with group id" should "get only MTU updates if no external resolver is set" in {
    val mtuRequests = Gen.listOfN(2, genCreateUpdate(genType = Gen.const(UpdateType.multi_target))).sample.get
    val externalUpdateRequests = Gen.listOfN(2, genCreateUpdate(genType = Gen.const(UpdateType.external))).sample.get
    val mtuIds = mtuRequests.map(createUpdateOk)
    externalUpdateRequests.foreach(createUpdateOk)

    val groupId = GroupId.generate()
    val devices = Gen.listOfN(100, genDeviceId).sample.get
    fakeRegistry.setGroup(groupId, devices)

    getUpdates(Some(groupId)) ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[Update]].values
      val sourceType = updates.map(_.source.sourceType).toSet
      sourceType.size shouldBe 1
      sourceType.head shouldBe UpdateType.multi_target
      updates.map(_.uuid) should contain allElementsOf mtuIds
    }
  }

  "GET to /updates with group id" should "forward to external resolver" in {
    val request = genCreateUpdate().sample.get
    val updateId = createUpdateOk(request)

    val groupId = GroupId.generate()

    val devices = Gen.listOfN(1024, genDeviceId).sample.get

    fakeRegistry.setGroup(groupId, devices)
    fakeUserProfile.setNamespaceSetting(testNs, testResolverUri)
    fakeResolver.setUpdates(testResolverUri, devices, List(request.updateSource.id))

    getUpdates(Some(groupId)) ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[Update]]

      updates.values should have size 1
      updates.values.map(_.uuid) should contain(updateId)
    }
  }

  "GET to /updates with group id for many devices" should "return updates for all devices" in {
    val requests = Gen.listOfN(10, genCreateUpdate()).sample.get
    val updateIds = requests.map(createUpdateOk)

    val groupId = GroupId.generate()

    val devices = Gen.listOfN(10, genDeviceId).sample.get

    fakeRegistry.setGroup(groupId, devices)
    fakeUserProfile.setNamespaceSetting(testNs, testResolverUri)

    requests.zip(devices).foreach { case (r, d) =>
      fakeResolver.setUpdates(testResolverUri, Seq(d), Seq(r.updateSource.id))
    }

    getUpdates(Some(groupId)) ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[Update]]

      updates.values should have size 10
      updates.values.map(_.uuid) contains allElementsOf(updateIds)
    }
  }

  "GET to /updates with group id" should "return all updates for a device" in {
    val requests = Gen.listOfN(10, genCreateUpdate()).sample.get
    val updateIds = requests.map(createUpdateOk)

    val groupId = GroupId.generate()

    val device = genDeviceId.sample.get

    fakeRegistry.setGroup(groupId, Seq(device))
    fakeUserProfile.setNamespaceSetting(testNs, testResolverUri)
    fakeResolver.setUpdates(testResolverUri, Seq(device), requests.map(_.updateSource.id))

    getUpdates(Some(groupId)) ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[Update]]

      updates.values should have size 10
      updates.values.map(_.uuid) contains allElementsOf(updateIds)
    }
  }

  "GET to /updates with group id for TOO many devices" should "return an error" in {
    val request = genCreateUpdate().sample.get
    createUpdateOk(request)

    val groupId = GroupId.generate()

    val devices = Gen.listOfN(5001, genDeviceId).sample.get.sortBy(_.uuid)

    fakeRegistry.setGroup(groupId, devices)
    fakeUserProfile.setNamespaceSetting(testNs, testResolverUri)
    fakeResolver.setUpdates(testResolverUri, Seq(devices.last), Seq(request.updateSource.id))

    getUpdates(Some(groupId)) ~> routes ~> check {
      status shouldBe InternalServerError
      val error = responseAs[ErrorRepresentation]
      error.code shouldBe ErrorCodes.TooManyRequestsToRemote
    }
  }

  "GET to /updates" should "get all existing updates" in {
    val requests = Gen.listOfN(2, genCreateUpdate()).sample.get
    val updateIds = requests.map(createUpdateOk)

    getUpdates() ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[Update]]
      updates.values.map(_.uuid) should contain allElementsOf updateIds
    }
  }

  "GET to /updates" should "get all updates sorted by name" in {
    val requests = Gen.listOfN(20, genCreateUpdate(Gen.alphaNumStr.retryUntil(_.nonEmpty))).sample.get
    val sortedNames = requests.map(_.name).sortBy(_.toLowerCase)
    requests.map(createUpdateOk)

    getUpdates() ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[Update]]
      updates.values.map(_.name).filter(sortedNames.contains) shouldBe sortedNames
    }
  }

  "GET to /updates?sortBy=createdAt" should "get all updates sorted from newest to oldest" in {
    val requests = Gen.listOfN(20, genCreateUpdate(Gen.alphaNumStr.retryUntil(_.nonEmpty))).sample.get
    requests.map(createUpdateOk)

    getUpdatesSorted(SortBy.CreatedAt) ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[Update]]
      updates.values.reverse.map(_.createdAt) shouldBe sorted
    }
  }

  "GET to /updates filtered by name" should "get only the campaigns that contain the filter parameter" in {
    val names = Seq("aabb", "baaxbc", "a123ba", "cba3b")
    val updatesIdNames = names
      .map(Gen.const)
      .map(genCreateUpdate(_).sample.get)
      .map(createUpdateOk)
      .map(getUpdateOk)
      .map(u => u.uuid -> u.name)
      .toMap

    val tests = Map("" -> names, "a1" -> Seq("a123ba"), "aa" -> Seq("aabb", "baaxbc"), "3b" -> Seq("a123ba", "cba3b"), "3" -> Seq("a123ba", "cba3b"))

    tests.foreach { case (nameContains, expected) =>
      val resultIds = getUpdatesOk(nameContains).values.map(_.uuid).filter(updatesIdNames.keySet.contains)
      val resultNames = updatesIdNames.filterKeys(resultIds.contains).values
      resultNames.size shouldBe expected.size
      resultNames should contain allElementsOf expected
    }
  }

  "GET to /updates filtered by groupId and name" should "fail with BadRequest" in {
    val groupId = GroupId.generate()
    val nameContains = arbitrary[String].sample.get
    getUpdates(Some(groupId), Some(nameContains)) ~> routes ~> check {
      status shouldBe BadRequest
    }
  }

  "GET to /updates/:updateId" should "return 404 Not Found if update does not exists" in {
    val updateId = genUpdateId.sample.get
    getUpdateResult(updateId) ~> check {
      status shouldBe NotFound
      responseAs[ErrorRepresentation].code shouldBe ErrorCodes.MissingUpdate
    }
  }

  "POST to /updates" should "create a new update" in {
    val request: CreateUpdate = genCreateUpdate().sample.get
    val updateId = createUpdateOk(request)
    val update = getUpdateResult(updateId) ~> check {
      status shouldBe OK
      responseAs[Update]
    }
    update.uuid shouldBe updateId
    request.name should equal(update.name)
    request.description should equal(update.description)
    request.updateSource should equal(update.source)
  }

  "Creating two updates with the same updateId" should "fail with Conflict error" in {
    val request1: CreateUpdate = genCreateUpdate().sample.get
    val request2: CreateUpdate = genCreateUpdate().sample.get.copy(updateSource= request1.updateSource)
    val updateId = createUpdateOk(request1)
    createUpdate(request2) ~> routes ~> check {
      status shouldBe Conflict
      import org.scalatest.OptionValues._
      responseAs[ErrorRepresentation].cause.flatMap(_.hcursor.get[UUID]("uuid").toOption).value should equal(updateId.uuid)

    }
  }

}
