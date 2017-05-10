package com.advancedtelematic.util

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestActorRef
import com.advancedtelematic.campaigner.actor.StatsCollector
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.http.Routes
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalatest.Suite

trait ResourceSpec extends ScalatestRouteTest
  with DatabaseSpec {
  self: Suite =>

  lazy val registry  = new FakeDeviceRegistryClient()
  lazy val director  = new FakeDirectorClient()
  lazy val collector = TestActorRef[StatsCollector]
  collector ! StatsCollector.Start()

  def apiUri(path: String): String = "/api/v1/" + path

  lazy val routes = new Routes(registry, director, collector).routes

}
