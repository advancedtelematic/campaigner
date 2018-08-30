package com.advancedtelematic.campaigner.actor

import akka.http.scaladsl.util.FastFuture
import akka.testkit.TestProbe
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, UpdateSupport}
import com.advancedtelematic.campaigner.util.{ActorSpec, CampaignerSpec, DatabaseUpdateSpecUtil}
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import org.scalacheck.Arbitrary

import scala.concurrent.Future

class CampaignSchedulerSpec extends ActorSpec[CampaignScheduler] with CampaignerSpec with UpdateSupport with DatabaseUpdateSpecUtil {
  import Arbitrary._
  import CampaignScheduler._

  import scala.concurrent.duration._

  val campaigns = Campaigns()

  override def beforeAll(): Unit = {
    super.beforeAll()
    deviceRegistry.clear()
  }

  "campaign scheduler" should "trigger updates for each group" in {
    val groups   = arbitrary[Set[GroupId]].sample.get
    val campaign = createDbCampaignWithUpdate(groups = groups).futureValue

    val parent   = TestProbe()

    campaigns.scheduleGroups(campaign.id, groups).futureValue
    groups.foreach { g => deviceRegistry.setGroup(g, arbitrary[Seq[DeviceId]].sample.get) }

    parent.childActorOf(CampaignScheduler.props(
      deviceRegistry,
      director,
      campaign,
      schedulerDelay,
      schedulerBatchSize
    ))
    parent.expectMsg(1.minute, CampaignComplete(campaign.id))

    deviceRegistry.allGroups() shouldBe groups
  }

  "PRO-3672: campaign with 0 affected devices" should "yield a `finished` status" in {
    val groups   = Set(arbitrary[GroupId].sample.get)
    val campaign = createDbCampaignWithUpdate(groups = groups).futureValue
    val parent   = TestProbe()

    val director = new DirectorClient {
      override def setMultiUpdateTarget(
        ns: Namespace,
        update: UpdateId,
        devices: Seq[DeviceId]
      ): Future[Seq[DeviceId]] = FastFuture.successful(Seq.empty)

      override def cancelUpdate(
        ns: Namespace,
        devs: Seq[DeviceId]
      ): Future[Seq[DeviceId]] = FastFuture.successful(Seq.empty)

      override def cancelUpdate(
        ns: Namespace,
        device: DeviceId): Future[Unit] = FastFuture.successful(())

      override def findAffected(ns: Namespace, updateId: UpdateId, devices: Seq[DeviceId]): Future[Seq[DeviceId]] =
        Future.successful(Seq.empty)
    }

    campaigns.scheduleGroups(campaign.id, groups).futureValue

    parent.childActorOf(CampaignScheduler.props(
      deviceRegistry,
      director,
      campaign,
      schedulerDelay,
      schedulerBatchSize
    ))
    parent.expectMsg(3.seconds, CampaignComplete(campaign.id))

    campaigns.campaignStats(campaign.id).futureValue.status shouldBe CampaignStatus.finished
  }
}
