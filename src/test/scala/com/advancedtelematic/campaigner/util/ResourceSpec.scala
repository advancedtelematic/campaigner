package com.advancedtelematic.campaigner.util

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.advancedtelematic.campaigner.http.Routes
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalatest.Suite

trait ResourceSpec extends ScalatestRouteTest
  with DatabaseSpec {
  self: Suite =>

  def apiUri(path: String): String = "/api/v1/" + path

  lazy val routes = new Routes().routes

}
