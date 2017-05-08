package com.advancedtelematic.util

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.http.Routes
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalatest.Suite

trait ResourceSpec extends ScalatestRouteTest with DatabaseSpec {
  self: Suite =>

  def apiUri(path: String): String = "/api/v1/" + path

  val deviceRegistry = new FakeDeviceRegistryClient()
  val director = new FakeDirectorClient()

  lazy val routes = new Routes(deviceRegistry, director).routes

}
