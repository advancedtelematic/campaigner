package com.advancedtelematic.campaigner.actor

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.PoisonPill
import akka.http.scaladsl.util.FastFuture
import akka.testkit.TestProbe
import cats.data.NonEmptyList
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{CampaignErrorsSupport, Campaigns, UpdateSupport}
import com.advancedtelematic.campaigner.util.{ActorSpec, CampaignerSpec, DatabaseUpdateSpecUtil, FakeDirectorClient}
import com.advancedtelematic.libats.data.DataType.{CorrelationId, Namespace}
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import org.scalacheck.{Arbitrary, Gen}

import scala.concurrent.Future

class CampaignSchedulerSpec extends ActorSpec[CampaignScheduler] with CampaignerSpec with UpdateSupport with DatabaseUpdateSpecUtil with CampaignErrorsSupport {
  import Arbitrary._
  import CampaignScheduler._

  import scala.concurrent.duration._

  val campaigns = Campaigns()

  override def beforeAll(): Unit = {
    super.beforeAll()
    deviceRegistry.clear()
  }

  "campaign scheduler" should "trigger updates for each group" in {
    val groups   = NonEmptyList.fromListUnsafe(Gen.listOfN(3, genGroupId).sample.get)
    val campaign = createDbCampaignWithUpdate(maybeGroups = Some(groups)).futureValue

    val parent   = TestProbe()

    campaigns.scheduleGroups(campaign.id, groups).futureValue
    groups.map{ g => deviceRegistry.setGroup(g, arbitrary[Seq[DeviceId]].sample.get) }

    val child = parent.childActorOf(CampaignScheduler.props(
      deviceRegistry,
      director,
      campaign,
      schedulerDelay,
      schedulerBatchSize
    ))

    parent.expectMsg(10.seconds, CampaignSchedulingComplete(campaign.id))

    child ! PoisonPill
  }

  "PRO-3672: campaign with 0 affected devices" should "yield a `finished` status" in {
    val groups   = NonEmptyList.fromListUnsafe(Gen.listOfN(3, genGroupId).sample.get)
    val campaign = createDbCampaignWithUpdate(maybeGroups = Some(groups)).futureValue
    val parent   = TestProbe()

    campaigns.scheduleGroups(campaign.id, groups).futureValue

    val child = parent.childActorOf(CampaignScheduler.props(
      deviceRegistry,
      new FakeDirectorClient,
      campaign,
      schedulerDelay,
      schedulerBatchSize
    ))

    parent.expectMsg(5.seconds, CampaignSchedulingComplete(campaign.id))

    child ! PoisonPill

    campaigns.campaignStats(campaign.id).futureValue.status shouldBe CampaignStatus.finished
  }

  it should "try again if campaign fails" in {
    val failingDirector = new DirectorClient {
      var fails = new AtomicInteger(0)

      override def setMultiUpdateTarget(ns: Namespace, updateId: ExternalUpdateId, devices: Seq[DeviceId], correlationId: CorrelationId): Future[Seq[DeviceId]] = {
        if(fails.incrementAndGet() >= 3)
          FastFuture.successful(devices)
        else
          throw new RuntimeException("[test] setMultiUpdateTarget failing director")
      }

      override def findAffected(ns: Namespace, updateId: ExternalUpdateId, devices: Seq[DeviceId]): Future[Seq[DeviceId]] =
        throw new RuntimeException("[test] findAffected failing director")

      override def cancelUpdate(ns: Namespace, devices: Seq[DeviceId]): Future[Seq[DeviceId]] = ???

      override def cancelUpdate(ns: Namespace, device: DeviceId): Future[Unit] = ???
    }

    val groups   = NonEmptyList.fromListUnsafe(Gen.listOfN(1, genGroupId).sample.get)
    val campaign = createDbCampaignWithUpdate(maybeGroups = Some(groups)).futureValue

    campaigns.scheduleGroups(campaign.id, groups).futureValue
    groups.map{ g => deviceRegistry.setGroup(g, arbitrary[Seq[DeviceId]].sample.get) }

    val parent   = TestProbe()

    val child = parent.childActorOf(CampaignScheduler.props(
      deviceRegistry,
      failingDirector,
      campaign,
      schedulerDelay,
      schedulerBatchSize
    ))

    parent.expectMsg(5.seconds, CampaignSchedulingComplete(campaign.id))

    child ! PoisonPill

    val campaignErrors = campaignErrorsRepo.find(campaign.id).futureValue.head

    campaignErrors.errorCount shouldBe 2
    campaignErrors.lastError shouldBe "[test] setMultiUpdateTarget failing director"
  }
}
