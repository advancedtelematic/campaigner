package com.advancedtelematic.campaigner.actor

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.actor.CampaignScheduler._
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.libats.data.Namespace
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfterAll, Matchers, PropSpecLike}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class CampaignSchedulerSpec extends TestKit(ActorSystem("CampaignScheduler"))
  with Matchers
  with PropertyChecks
  with PropSpecLike
  with Settings
  with BeforeAndAfterAll {

  implicit lazy val ec: ExecutionContext = system.dispatcher
  lazy val registry = new FakeDeviceRegistryClient()
  lazy val director = new FakeDirectorClient()

  def clearClientState()(implicit ec: ExecutionContext) {
    registry.state.clear()
    director.state.clear()
  }

  property("campaign scheduler should trigger updates for each group") {
    forAll { (ns: Namespace,
              update: UpdateId,
              grps: Set[GroupId]) =>

      clearClientState()

      val parent = TestProbe()
      val props = CampaignScheduler.props(registry, director, schedulerDelay, schedulerBatchSize, ns, update)
      val ref = parent.childActorOf(props)
      ref ! ScheduleCampaign(grps)
      parent.expectMsg(CampaignComplete(update))
      registry.state.keys.asScala.toSet shouldBe grps
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
    system.terminate()
    ()
  }

}
