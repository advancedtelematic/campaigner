package com.advancedtelematic.campaigner.actor

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import com.advancedtelematic.campaigner.actor.GroupScheduler._
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.libats.data.Namespace
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{BeforeAndAfterAll, Matchers, PropSpecLike}
import scala.concurrent.ExecutionContext

class GroupSchedulerSpec extends TestKit(ActorSystem("GroupScheduler"))
  with Matchers
  with PropertyChecks
  with PropSpecLike
  with BeforeAndAfterAll {

  import scala.concurrent.duration._

  implicit lazy val ec: ExecutionContext = system.dispatcher
  lazy val registry = new FakeDeviceRegistryClient()
  lazy val director = new FakeDirectorClient()

  def clearClientState()(implicit ec: ExecutionContext) {
    registry.state.clear()
    director.state.clear()
  }

  property("group scheduler should trigger updates for each device in batch") {
    forAll { (ns: Namespace,
             update: UpdateId,
             grp: GroupId) =>
      forAll(Gen.posNum[Int]) { batchSize =>

        clearClientState()

        val parent = TestProbe()
        val props  = GroupScheduler.props(registry, director, batchSize.longValue, ns, update, grp)
        val ref    = parent.childActorOf(props)
        ref ! LaunchBatch()
        parent.expectMsgPF(1.seconds) {
          case BatchComplete(_grp, 0) if _grp == grp => true
          case GroupComplete(_grp)    if _grp == grp => true
        }
        director.state.get(update).toSet.subsetOf(
          registry.state.get(grp).take(batchSize).toSet
        ) shouldBe true
      }
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
    system.terminate()
    ()
  }

}
