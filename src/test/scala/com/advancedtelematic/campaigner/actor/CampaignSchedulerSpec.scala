package com.advancedtelematic.campaigner.actor

import akka.http.scaladsl.util.FastFuture
import akka.testkit.TestProbe
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.campaigner.util.{ActorSpec, CampaignerSpec}
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import org.scalacheck.Arbitrary
import scala.collection.JavaConverters._
import scala.concurrent.Future

class CampaignSchedulerSpec extends ActorSpec[CampaignScheduler] with CampaignerSpec {

  import Arbitrary._
  import CampaignScheduler._
  import scala.concurrent.duration._

  val campaigns = Campaigns()

  "campaign scheduler" should "trigger updates for each group" in {

    val campaign = arbitrary[Campaign].sample.get
    val groups   = arbitrary[Set[GroupId]].sample.get
    val parent   = TestProbe()

    campaigns.create(campaign, groups, Seq.empty).futureValue
    campaigns.scheduleGroups(campaign.namespace, campaign.id, groups).futureValue

    parent.childActorOf(CampaignScheduler.props(
      registry,
      director,
      campaign,
      schedulerDelay,
      schedulerBatchSize
    ))
    parent.expectMsg(1.minute, CampaignComplete(campaign.id))
    registry.state.keys.asScala.toSet shouldBe groups
  }

  "PRO-3672: campaign with 0 affected devices" should "yield a `finished` status" in {

    val campaign = arbitrary[Campaign].sample.get
    val groups   = Set(arbitrary[GroupId].sample.get)
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

    campaigns.create(campaign, groups, Seq.empty).futureValue
    campaigns.scheduleGroups(campaign.namespace, campaign.id, groups).futureValue

    parent.childActorOf(CampaignScheduler.props(
      registry,
      director,
      campaign,
      schedulerDelay,
      schedulerBatchSize
    ))
    parent.expectMsg(3.seconds, CampaignComplete(campaign.id))

    campaigns.campaignStats(campaign.namespace, campaign.id)
      .futureValue.status shouldBe CampaignStatus.finished
  }

}
