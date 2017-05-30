package com.advancedtelematic.util

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestActorRef
import com.advancedtelematic.campaigner.actor.CampaignSupervisor
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.http.Routes
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalatest.Suite

trait ResourceSpec extends ScalatestRouteTest
  with DatabaseSpec {
  self: Suite =>

  lazy val registry  = new FakeDeviceRegistryClient()
  lazy val director  = new FakeDirectorClient()
  lazy val supervisor = TestActorRef(new CampaignSupervisor(registry, director))

  def apiUri(path: String): String = "/api/v1/" + path

  lazy val routes = new Routes(supervisor).routes

}
