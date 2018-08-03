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
import org.scalatest.BeforeAndAfterEach

class UpdateResourceSpec extends CampaignerSpec with ResourceSpec with UpdateSupport with BeforeAndAfterEach {

  val updates = Updates()

  override protected def afterEach(): Unit = {
    super.afterEach()
    // To avoid aleatory integrity violations on the unique key (namespace, externalId).
    updates.clear()
  }

  private def createUpdate(request: CreateUpdate): HttpRequest =
    Post(apiUri("updates"), request).withHeaders(header)

  private def createUpdateOk(request: CreateUpdate): UpdateId =
    createUpdate(request) ~> routes ~> check {
      status shouldBe OK
      responseAs[UpdateId]
    }

  private def getUpdates: HttpRequest = Get(apiUri("updates")).withHeaders(header)

  "POST to /updates" should "create a new update" in {
    val request: CreateUpdate = genCreateUpdate.sample.get
    createUpdate(request) ~> routes ~> check {
      status shouldBe OK
    }
  }

  "GET to /updates" should "get all existing updates" in {
    // Make sure the external IDs are different to avoid aleatory integrity violations on the unique key (namespace, externalId).
    val request1: CreateUpdate = genCreateUpdate.sample.get
    val request2: CreateUpdate = genCreateUpdate.suchThat(_.externalId match {
      case None => request1.externalId.isDefined
      case Some (e) => request1.externalId.isEmpty || request1.externalId.get != e
    }).sample.get
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

  "Creating two updates with the same externalId" should "fail with Conflict error" in {
    val request1: CreateUpdate = genCreateUpdate.sample.get
    val request2: CreateUpdate = genCreateUpdate.sample.get.copy(externalId = request1.externalId)
    createUpdateOk(request1)
    createUpdate(request2) ~> routes ~> check {
      status shouldBe Conflict
    }
  }

}
