package com.advancedtelematic.campaigner.util

import org.scalactic.source
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType.SortBy.SortBy
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.http.Routes
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.test.DatabaseSpec
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import org.scalatest.Suite
import org.scalatest.time.{Seconds, Span}

trait ResourceSpec extends ScalatestRouteTest
  with DatabaseSpec {
  self: Suite with CampaignerSpec =>

  implicit val defaultTimeout = RouteTestTimeout(Span(5, Seconds))

  def apiUri(path: String): Uri = "/api/v2/" + path

  def testNs = Namespace("testNs")

  def header = RawHeader("x-ats-namespace", testNs.get)

  def testResolverUri = Uri("http://test.com")

  val fakeDirector = new FakeDirectorClient
  val fakeRegistry = new FakeDeviceRegistry
  val fakeUserProfile = new FakeUserProfileClient
  val fakeResolver = new FakeResolverClient

  lazy val routes: Route = new Routes(fakeDirector, fakeRegistry, fakeResolver, fakeUserProfile).routes

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

  def getCampaignsOk(campaignStatus: Option[CampaignStatus] = None, sortBy: Option[SortBy] = None)(implicit pos: source.Position): PaginationResult[CampaignId] = {
    val m = List("status" -> campaignStatus, "sortBy" -> sortBy).collect { case (k, Some(v)) => k -> v.toString }.toMap

    Get(apiUri("campaigns").withQuery(Query(m))).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      responseAs[PaginationResult[CampaignId]]
    }
  }
}

