package com.advancedtelematic.campaigner.actor

import akka.actor.ActorSystem
import akka.http.scaladsl.util.FastFuture
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.libats.data.Namespace
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import scala.concurrent.{ExecutionContext, Future}

class GroupSchedulerSpec extends TestKit(ActorSystem("GroupSchedulerSpec"))
  with Matchers
  with FlatSpecLike
  with Settings
  with BeforeAndAfterAll {

  import Arbitrary._
  import GroupScheduler._
  import StatsCollector._
  import scala.concurrent.duration._

  implicit lazy val ec: ExecutionContext = system.dispatcher
  val batch = schedulerBatchSize
  lazy val registry = new FakeDeviceRegistryClient()
  lazy val director = new FakeDirectorClient()

  def clearClientState()(implicit ec: ExecutionContext) {
    registry.state.clear()
    director.state.clear()
  }

  "group scheduler" should "trigger updates for each device in batch" in {

    val ns     = arbitrary[Namespace].sample.get
    val update = arbitrary[UpdateId].sample.get
    val grp    = arbitrary[GroupId].sample.get

    val parent = TestProbe()
    val props  = GroupScheduler.props(registry, director, ns, update, grp)
    val ref    = parent.childActorOf(props)
    ref ! LaunchBatch()
    parent.expectMsgPF(3.seconds) {
      case BatchComplete(_grp, stats)
        if _grp == grp &&
           stats.processed == batch &&
           stats.affected <= stats.processed
           => ()
      case GroupComplete(_grp, stats)
        if _grp == grp &&
           stats.processed == registry.state.get(grp).length &&
           stats.affected  == director.state.get(update).size
           => ()
    }
    director.state.get(update).toSet.subsetOf(
      registry.state.get(grp).take(batch).toSet
    ) shouldBe true
  }

  "group scheduler" should "respect groups with processed devices > batch size" in {
    val ns     = arbitrary[Namespace].sample.get
    val update = arbitrary[UpdateId].sample.get
    val grp    = arbitrary[GroupId].sample.get
    val n      = Gen.choose(batch, batch * 10).sample.get
    val devs   = Gen.listOfN(n, genDeviceId).sample.get
    val rem    = n % batch

    var stats  = Stats(0, 0)

    val registry = new DeviceRegistry {
      override def getDevicesInGroup(_ns: Namespace,
                                     _grp: GroupId,
                                     offset: Int,
                                     limit: Int): Future[Seq[DeviceId]] =
        FastFuture.successful(devs.drop(offset).take(limit))
    }

    implicit val timeout = Timeout(3.seconds)
    clearClientState()

    val parent = TestProbe()
    val props  = GroupScheduler.props(registry, director, ns, update, grp)
    val ref    = parent.childActorOf(props)
    ref ! LaunchBatch()

    Range(0, n/batch).foreach { i =>
      parent.expectMsgPF(timeout.duration) {
        case msg@BatchComplete(grp, Stats(batch, _)) => stats = statsMonoid.combine(stats, msg.stats)
      }
      ref ! LaunchBatch()
    }
    parent.expectMsgPF(timeout.duration) {
      case msg@GroupComplete(grp, Stats(rem, _)) => stats = statsMonoid.combine(stats, msg.stats)
    }
    director.state.get(update).toSet.subsetOf(
      devs.toSet
    ) shouldBe true
  }

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
    system.terminate()
    ()
  }

}
