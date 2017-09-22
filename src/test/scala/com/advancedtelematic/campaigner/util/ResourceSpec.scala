package com.advancedtelematic.campaigner.util

import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.advancedtelematic.campaigner.client.FakeDirectorClient
import com.advancedtelematic.campaigner.http.Routes
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalatest.Suite
import org.scalatest.time.{Seconds, Span}

trait ResourceSpec extends ScalatestRouteTest
  with DatabaseSpec {
  self: Suite =>

  implicit val defaultTimeout = RouteTestTimeout(Span(5, Seconds))

  def apiUri(path: String): String = "/api/v2/" + path
  val director = new FakeDirectorClient

  lazy val routes = new Routes(director).routes
}
