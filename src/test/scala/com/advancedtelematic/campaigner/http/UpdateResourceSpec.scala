package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.Location
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.{CreateUpdate, Update}
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.UpdateSupport
import com.advancedtelematic.campaigner.util.{CampaignerSpec, ResourceSpec}
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.UpdateId
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

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

  "GET to /updates" should "get all existing updates" in {
    // Make sure the external IDs are different to avoid aleatory integrity violations on the unique key (namespace, externalId).
    val request1: CreateUpdate = genCreateUpdate.sample.get
    val request2: CreateUpdate = genCreateUpdate.sample.get
    val updateId1 = createUpdateOk(request1)
    val updateId2 = createUpdateOk(request2)
    getUpdates ~> routes ~> check {
      status shouldBe OK
      val updates = responseAs[PaginationResult[Update]]
      updates.total shouldBe 2
      updates.values.find(_.uuid == updateId1) shouldBe defined
      updates.values.find(_.uuid == updateId2) shouldBe defined
    }
  }

  "POST to /updates" should "create a new update" in {
    val request: CreateUpdate = genCreateUpdate.sample.get
    createUpdateOk(request)
  }

  "Creating two updates with the same updateId" should "fail with Conflict error" in {
    val request1: CreateUpdate = genCreateUpdate.sample.get
    val request2: CreateUpdate = genCreateUpdate.sample.get.copy(updateSource= request1.updateSource)
    createUpdateOk(request1)
    createUpdate(request2) ~> routes ~> check {
      status shouldBe Conflict
    }
  }

}
