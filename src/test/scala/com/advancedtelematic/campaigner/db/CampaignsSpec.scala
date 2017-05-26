package com.advancedtelematic.campaigner.db

import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.http.Errors._
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncFlatSpec, Matchers}

class CampaignsSpec extends AsyncFlatSpec
    with CampaignSupport
    with DatabaseSpec
    with Matchers
    with ScalaFutures {

  import Arbitrary._

  "complete batch" should "fail if the campaign does not exist" in {
    recoverToSucceededIf[CampaignMissing.type] {
      Campaigns.completeBatch(
        arbitrary[Namespace].sample.get,
        CampaignId.generate(),
        GroupId.generate(),
        Stats(0, 0)
      )
    }
  }

  "complete batch" should "update campaign stats for a group" in {

    val campaign  = arbitrary[Campaign].sample.get
    val group     = GroupId.generate()
    val processed = Gen.posNum[Long].sample.get
    val affected  = Gen.chooseNum[Long](0, processed).sample.get

    for {
      _ <- Campaigns.persist(campaign, Set(group))
      _ <- Campaigns.completeBatch(
        campaign.namespace,
        campaign.id,
        group,
        Stats(processed, affected)
      )
      status <- Campaigns.groupStatusFor(campaign.id, group)
      stats <- Campaigns.campaignStatsFor(campaign.namespace, campaign.id)
    } yield {
      status shouldBe Some(GroupStatus.scheduled)
      stats  shouldBe Map(group -> Stats(processed, affected))
    }
  }

  "complete group" should "fail if the campaign does not exist" in {
    recoverToSucceededIf[CampaignMissing.type] {
      Campaigns.completeGroup(
        arbitrary[Namespace].sample.get,
        CampaignId.generate(),
        GroupId.generate(),
        Stats(processed = 0, affected = 0)
      )
    }
  }

  "complete group" should "complete campaign stats for a group" in {

    val campaign  = arbitrary[Campaign].sample.get
    val group     = GroupId.generate()
    val processed = Gen.posNum[Long].sample.get
    val affected  = Gen.chooseNum[Long](0, processed).sample.get

    for {
      _ <- Campaigns.persist(campaign, Set(group))
      _ <- Campaigns.completeGroup(
        campaign.namespace,
        campaign.id,
        group,
        Stats(processed, affected)
      )
      status <- Campaigns.groupStatusFor(campaign.id, group)
      stats  <- Campaigns.campaignStatsFor(campaign.namespace, campaign.id)
    } yield {
      status shouldBe Some(GroupStatus.launched)
      stats  shouldBe Map(group -> Stats(processed, affected))
    }
  }

  "finishing one device" should "work with several campaigns" in {

    val ns        = arbitrary[Namespace].sample.get
    val campaigns = arbitrary[Seq[Campaign]].sample.get
    val update    = UpdateId.generate()
    val group     = GroupId.generate()
    val device    = DeviceId.generate()

    for {
      _ <- FastFuture.traverse(campaigns)(c => Campaigns.persist(c.copy(namespace = ns, updateId = update), Set(group)))
      _ <- FastFuture.traverse(campaigns)(c => Campaigns.scheduleDevice(c.id, update, device))
      _ <- Campaigns.finishDevice(update, device, DeviceStatus.successful)
      c <- Campaigns.countFinished(ns, campaigns(0).id)
    } yield c shouldBe 1
  }

  "finishing devices" should "work with one campaign" in {

    val campaign = arbitrary[Campaign].sample.get
    val group    = GroupId.generate()
    val devices  = arbitrary[Seq[DeviceId]].sample.get

    for {
      _ <- Campaigns.persist(campaign, Set(group))
      _ <- FastFuture.traverse(devices)(d => Campaigns.scheduleDevice(campaign.id, campaign.updateId, d))
      _ <- FastFuture.traverse(devices)(d => Campaigns.finishDevice(campaign.updateId, d, DeviceStatus.failed))
      c <- Campaigns.countFinished(campaign.namespace, campaign.id)
    } yield c shouldBe devices.length
  }

}
