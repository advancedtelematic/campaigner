package com.advancedtelematic.campaigner.util

import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.advancedtelematic.campaigner.client.FakeDirectorClient
import com.advancedtelematic.campaigner.data.DataType.{CampaignId, CreateCampaign, GetCampaign}
import com.advancedtelematic.campaigner.http.Routes
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalatest.Suite
import org.scalatest.time.{Seconds, Span}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import cats.syntax.show._
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.campaigner.data.Codecs._

trait ResourceSpec extends ScalatestRouteTest
  with DatabaseSpec {
  self: Suite with CampaignerSpec =>

  implicit val defaultTimeout = RouteTestTimeout(Span(5, Seconds))

  def apiUri(path: String): String = "/api/v2/" + path
  val director = new FakeDirectorClient

  def testNs = Namespace("testNs")

  def header = RawHeader("x-ats-namespace", testNs.get)

  lazy val routes = new Routes(director).routes

  def createCampaignOk(request: CreateCampaign): CampaignId =
    Post(apiUri("campaigns"), request).withHeaders(header) ~> routes ~> check {
      status shouldBe Created
      responseAs[CampaignId]
    }

  def getCampaignOk(id: CampaignId): GetCampaign =
    Get(apiUri("campaigns/" + id.show)).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      responseAs[GetCampaign]
    }

  def getCampaignsOk(): PaginationResult[CampaignId] =
    Get(apiUri("campaigns")).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      responseAs[PaginationResult[CampaignId]]
    }
}
