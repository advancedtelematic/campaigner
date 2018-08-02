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

  def createUpdateOk(request: CreateUpdate): UpdateId =
    createUpdate(request) ~> routes ~> check {
      status shouldBe OK
      responseAs[UpdateId]
    }

  def createUpdate(request: CreateUpdate): HttpRequest =
    Post(apiUri("updates"), request).withHeaders(header)

  def getUpdates: HttpRequest = Get(apiUri("updates")).withHeaders(header)

  "GET to /updates" should "get all existing updates" in {
    val request1 = genCreateUpdate.sample.get
    val request2 = genCreateUpdate.sample.get
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
    val request = genCreateUpdate.sample.get
    createUpdate(request) ~> routes ~> check {
      status shouldBe OK
    }
  }

}
