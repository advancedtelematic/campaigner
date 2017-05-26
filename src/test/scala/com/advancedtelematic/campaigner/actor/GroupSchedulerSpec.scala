package com.advancedtelematic.campaigner.actor

import akka.actor.ActorSystem
import akka.http.scaladsl.util.FastFuture
import akka.testkit.{TestKit, TestProbe}
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.CampaignSupport
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libats.test.DatabaseSpec
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import scala.concurrent.Future

class GroupSchedulerSpec extends TestKit(ActorSystem("GroupSchedulerSpec"))
  with CampaignSupport
  with FlatSpecLike
  with Matchers
  with ScalaFutures
  with Settings
  with BeforeAndAfterAll
  with DatabaseSpec {

  import Arbitrary._
  import GroupScheduler._
  import scala.concurrent.duration._

  val batch = schedulerBatchSize.toInt
  lazy val registry = new FakeDeviceRegistryClient()
  lazy val director = new FakeDirectorClient()
  implicit lazy val ec = system.dispatcher

  def clearClientState() = {
    registry.state.clear()
    director.state.clear()
  }

  "group scheduler" should "trigger updates for each device in batch" in {
    val campaign = arbitrary[Campaign].sample.get
    val group    = GroupId.generate()
    val parent = TestProbe()
    val props  = GroupScheduler.props(registry, director, 10.minutes, schedulerBatchSize, campaign, group)

    clearClientState()
    Campaigns.persist(campaign, Set(group)).futureValue

    parent.childActorOf(props)
    parent.expectMsgPF(10.seconds) {
      case BatchComplete(grp, _) => grp
      case GroupComplete(grp)    => grp
    }
    registry.state.get(group).take(batch).toSet should contain allElementsOf (
      director.state.get(campaign.updateId).toSet
    )
  }

  "group scheduler" should "respect groups with processed devices > batch size" in {
    val campaign = arbitrary[Campaign].sample.get
    val group    = GroupId.generate()
    val n        = Gen.choose(batch, batch * 10).sample.get
    val devs     = Gen.listOfN(n, genDeviceId).sample.get

    Campaigns.persist(campaign, Set(group)).futureValue

    val registry = new DeviceRegistryClient {
      override def devicesInGroup(_ns: Namespace,
                                  _grp: GroupId,
                                  offset: Long,
                                  limit: Long): Future[Seq[DeviceId]] =
        FastFuture.successful(devs.drop(offset.toInt).take(limit.toInt))
    }

    clearClientState()

    val parent = TestProbe()
    val props  = GroupScheduler.props(registry, director, schedulerDelay, schedulerBatchSize, campaign, group)
    parent.childActorOf(props)

    Range(0, n/batch).foreach { i =>
      parent.expectMsg(BatchComplete(group, (i+1).toLong * batch))
    }
    parent.expectMsg(GroupComplete(group))
    director.state.get(campaign.updateId).toSet.subsetOf(
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
