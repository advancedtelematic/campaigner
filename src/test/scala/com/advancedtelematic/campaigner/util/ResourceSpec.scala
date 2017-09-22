package com.advancedtelematic.campaigner.util

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.advancedtelematic.campaigner.client.FakeDirectorClient
import com.advancedtelematic.campaigner.http.Routes
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalatest.Suite

trait ResourceSpec extends ScalatestRouteTest
  with DatabaseSpec {
  self: Suite =>

  def apiUri(path: String): String = "/api/v2/" + path
  val director = new FakeDirectorClient

  lazy val routes = new Routes(director).routes

}
