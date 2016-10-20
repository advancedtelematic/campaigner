package com.advancedtelematic.util

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.advancedtelematic.service_blueprint.http.ServiceBlueprintRoutes
import org.scalatest.Suite

trait ResourceSpec extends ScalatestRouteTest with DatabaseSpec {
  self: Suite =>

  implicit val _db = db

  def apiUri(path: String): String = "/api/v1/" + path

  lazy val routes = new ServiceBlueprintRoutes().routes
}


