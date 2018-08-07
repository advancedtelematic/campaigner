package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.StatusCodes._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.CreateUpdate
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{UpdateSupport, Updates}
import com.advancedtelematic.campaigner.util.{CampaignerSpec, ResourceSpec}
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.UpdateId
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

class UpdateResourceSpec extends CampaignerSpec with ResourceSpec with UpdateSupport {

  val updates = Updates()

  private def createUpdate(request: CreateUpdate): HttpRequest =
    Post(apiUri("updates"), request).withHeaders(header)

  private def createUpdateOk(request: CreateUpdate): UpdateId =
    createUpdate(request) ~> routes ~> check {
      status shouldBe OK
      responseAs[UpdateId]
    }

  private def getUpdates: HttpRequest = Get(apiUri("updates")).withHeaders(header)

  "GET to /updates" should "get all existing updates" in {
    // Make sure the external IDs are different to avoid aleatory integrity violations on the unique key (namespace, externalId).
    val request1: CreateUpdate = genCreateUpdate.sample.get
    val request2: CreateUpdate = genCreateUpdate.sample.get
    val updateId1 = createUpdateOk(request1)
    val updateId2 = createUpdateOk(request2)
    getUpdates ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[UpdateId]]
      updates.total shouldBe 2
      updates.values should contain(updateId1)
      updates.values should contain(updateId2)
    }
  }

  "POST to /updates" should "create a new update" in {
    val request: CreateUpdate = genCreateUpdate.sample.get
    createUpdate(request) ~> routes ~> check {
      status shouldBe OK
    }
  }

  "Creating two updates without updateId" should "be allowed" in {
    val request1: CreateUpdate = genCreateUpdate.sample.get.copy(updateId = None)
    val request2: CreateUpdate = genCreateUpdate.sample.get.copy(updateId = None)
    createUpdateOk(request1)
    createUpdate(request2) ~> routes ~> check {
      status shouldBe OK
    }
  }

  "Creating two updates with the same updateId" should "fail with Conflict error" in {
    val request1: CreateUpdate = genCreateUpdate.retryUntil(_.updateId.isDefined).sample.get
    val request2: CreateUpdate = genCreateUpdate.sample.get.copy(updateId = request1.updateId)
    createUpdateOk(request1)
    createUpdate(request2) ~> routes ~> check {
      status shouldBe Conflict
    }
  }

}
