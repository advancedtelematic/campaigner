package com.advancedtelematic.campaigner.http

import java.util.UUID

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.Location
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.GroupId._
import com.advancedtelematic.campaigner.data.DataType.{CreateUpdate, GroupId, Update}
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.UpdateSupport
import com.advancedtelematic.campaigner.util.{CampaignerSpec, ResourceSpec}
import com.advancedtelematic.libats.data.{ErrorRepresentation, PaginationResult}
import com.advancedtelematic.libats.messaging_datatype.DataType.UpdateId
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
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

  private def getUpdates: HttpRequest = Get(apiUri("updates")).withHeaders(header)

  private def getUpdateResult(id: UpdateId): RouteTestResult = {
    Get(apiUri(s"updates/${id.uuid.toString}")).withHeaders(header) ~> routes
  }

  private def getGroupUpdates(groupId: GroupId) =
    Get(apiUri("updates").withQuery(Query("groupId" -> groupId.show))).withHeaders(header)

  "GET to /updates with group id" should "forwards to external resolver" in {
    val request = genCreateUpdate.sample.get
    val updateId = createUpdateOk(request)

    val groupId = GroupId.generate()

    val devices = Gen.listOfN(1024, genDeviceId).sample.get

    fakeRegistry.setGroup(groupId, devices)

    fakeResolver.setUpdates(devices, List(request.updateSource.id))

    getGroupUpdates(groupId) ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[Update]]

      updates.values should have size(1)
      updates.values.map(_.uuid) should contain(updateId)
    }
  }

  "GET to /updates with group id for many devices" should "returns updates for all devices" in {
    val requests = Gen.listOfN(10, genCreateUpdate).sample.get
    val updateIds = requests.map(createUpdateOk)

    val groupId = GroupId.generate()

    val devices = Gen.listOfN(10, genDeviceId).sample.get

    fakeRegistry.setGroup(groupId, devices)

    requests.zip(devices).foreach { case (r, d) =>
      fakeResolver.setUpdates(Seq(d), Seq(r.updateSource.id))
    }

    getGroupUpdates(groupId) ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[Update]]

      updates.values should have size(10)
      updates.values.map(_.uuid) contains allElementsOf(updateIds)
    }
  }

  "GET to /updates with group id" should "returns all updates for a device" in {
    val requests = Gen.listOfN(10, genCreateUpdate).sample.get
    val updateIds = requests.map(createUpdateOk)

    val groupId = GroupId.generate()

    val device = genDeviceId.sample.get

    fakeRegistry.setGroup(groupId, Seq(device))

    fakeResolver.setUpdates(Seq(device), requests.map(_.updateSource.id))

    getGroupUpdates(groupId) ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[Update]]

      updates.values should have size(10)
      updates.values.map(_.uuid) contains allElementsOf(updateIds)
    }
  }

  "GET to /updates with group id for TOO many devices" should "return an error" in {
    val request = genCreateUpdate.sample.get
    createUpdateOk(request)

    val groupId = GroupId.generate()

    val devices = Gen.listOfN(5001, genDeviceId).sample.get.sortBy(_.uuid)

    fakeRegistry.setGroup(groupId, devices)

    fakeResolver.setUpdates(Seq(devices.last), Seq(request.updateSource.id))

    getGroupUpdates(groupId) ~> routes ~> check {
      status shouldBe InternalServerError
      val error = responseAs[ErrorRepresentation]
      error.code shouldBe ErrorCodes.TooManyRequestsToRemote
    }
  }

  "GET to /updates" should "get all existing updates" in {
    // Make sure the external IDs are different to avoid random integrity violations on the unique key (namespace, externalId).
    val requests = Gen.listOfN(2, genCreateUpdate).sample.get
    val updateIds = requests.map(createUpdateOk)

    getUpdates ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[Update]]
      updates.values.map(_.uuid) should contain allElementsOf(updateIds)
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
    val request: CreateUpdate = genCreateUpdate.sample.get
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
    val request1: CreateUpdate = genCreateUpdate.sample.get
    val request2: CreateUpdate = genCreateUpdate.sample.get.copy(updateSource= request1.updateSource)
    val updateId = createUpdateOk(request1)
    createUpdate(request2) ~> routes ~> check {
      status shouldBe Conflict
      import org.scalatest.OptionValues._
      responseAs[ErrorRepresentation].cause.flatMap(_.hcursor.get[UUID]("uuid").toOption).value should equal(updateId.uuid)

    }
  }

}
