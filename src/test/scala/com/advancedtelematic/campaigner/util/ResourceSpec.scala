package com.advancedtelematic.campaigner.util

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpRequest, Uri}
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
import com.advancedtelematic.libats.messaging.MessageBusPublisher
import com.advancedtelematic.libats.test.DatabaseSpec
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Json
import org.scalactic.source
import org.scalatest.Suite
import org.scalatest.time.{Seconds, Span}

trait ResourceSpec extends ScalatestRouteTest
  with DatabaseSpec {
  self: Suite with CampaignerSpec =>

  implicit val defaultTimeout: RouteTestTimeout = RouteTestTimeout(Span(5, Seconds))

  def apiUri(path: String): Uri = "/api/v2/" + path

  def testNs = Namespace("testNs")

  def header = RawHeader("x-ats-namespace", testNs.get)

  def testResolverUri = Uri("http://test.com")

  val fakeDirector = new FakeDirectorClient
  val fakeRegistry = new FakeDeviceRegistry
  val fakeUserProfile = new FakeUserProfileClient
  val fakeResolver = new FakeResolverClient
  val messageBus = MessageBusPublisher.ignore

  lazy val routes: Route = new Routes(fakeDirector, fakeRegistry, fakeResolver, fakeUserProfile, messageBus).routes

  def createCampaign(request: Json): HttpRequest =
    Post(apiUri("campaigns"), request).withHeaders(header)

  def createCampaign(request: CreateCampaign): HttpRequest =
    Post(apiUri("campaigns"), request).withHeaders(header)

  def createCampaignOk(request: CreateCampaign): CampaignId =
    createCampaign(request) ~> routes ~> check {
      status shouldBe Created
      responseAs[CampaignId]
    }

  def getCampaignOk(id: CampaignId): GetCampaign =
    Get(apiUri("campaigns/" + id.show)).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      responseAs[GetCampaign]
    }

  def getCampaignsOk(campaignStatus: Option[CampaignStatus] = None, nameContains: Option[String] = None, sortBy: Option[SortBy] = None)
                    (implicit pos: source.Position): PaginationResult[CampaignId] = {
    val m = List("status" -> campaignStatus, "nameContains" -> nameContains, "sortBy" -> sortBy)
      .collect { case (k, Some(v)) => k -> v.toString }.toMap

    Get(apiUri("campaigns").withQuery(Query(m))).withHeaders(header) ~> routes ~> check {
      status shouldBe OK
      responseAs[PaginationResult[CampaignId]]
    }
  }
}

