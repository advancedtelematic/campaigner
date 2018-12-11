package com.advancedtelematic.campaigner.db

import akka.http.scaladsl.util.FastFuture
import cats.data.NonEmptyList
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.http.Errors._
import com.advancedtelematic.campaigner.util.DatabaseUpdateSpecUtil
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future

class CampaignsSpec extends AsyncFlatSpec
  with DatabaseSpec
  with Matchers
  with ScalaFutures
  with CampaignSupport
  with GroupStatsSupport
  with UpdateSupport
  with DatabaseUpdateSpecUtil {

  import Arbitrary._

  val campaigns = Campaigns()

  "count campaigns" should "return a list of how many campaigns there are for each status" in {
    val statuses = Seq(launched, finished, finished, cancelled, cancelled, cancelled)
    val cs = statuses.map(s => genCampaign.sample.get.copy(status = s))
    for {
      _ <- Future.sequence(cs.map(c => createDbCampaignWithUpdate(Some(c))))
      res <- campaigns.countByStatus
    } yield res shouldBe Map(prepared -> 0, launched -> 1, finished -> 2, cancelled -> 3)
  }

  "complete batch" should "fail if the campaign does not exist" in {
    recoverToSucceededIf[CampaignMissing.type] {
      campaigns.completeBatch(
        CampaignId.generate(),
        GroupId.generate(),
        Stats(0, 0)
      )
    }
  }

  "complete batch" should "update campaign stats for a group" in {
    val group     = GroupId.generate()
    val processed = Gen.posNum[Long].sample.get
    val affected  = Gen.chooseNum[Long](0, processed).sample.get

    for {
      campaign <- createDbCampaignWithUpdate()
      _ <- campaigns.completeBatch(
        campaign.id,
        group,
        Stats(processed, affected)
      )
      status <- groupStatsRepo.groupStatusFor(campaign.id, group)
      stats <- campaigns.campaignStatsFor(campaign.id)
    } yield {
      status shouldBe Some(GroupStatus.scheduled)
      stats  shouldBe Map(group -> Stats(processed, affected))
    }
  }

  "complete group" should "fail if the campaign does not exist" in {
    recoverToSucceededIf[CampaignMissing.type] {
      campaigns.completeGroup(
        CampaignId.generate(),
        GroupId.generate(),
        Stats(processed = 0, affected = 0)
      )
    }
  }

  "complete group" should "complete campaign stats for a group" in {

    val group     = GroupId.generate()
    val processed = Gen.posNum[Long].sample.get
    val affected  = Gen.chooseNum[Long](0, processed).sample.get

    for {
      campaign <- createDbCampaignWithUpdate()
      _ <- campaigns.completeGroup(
        campaign.id,
        group,
        Stats(processed, affected)
      )
      status <- groupStatsRepo.groupStatusFor(campaign.id, group)
      stats  <- campaigns.campaignStatsFor(campaign.id)
    } yield {
      status shouldBe Some(GroupStatus.launched)
      stats  shouldBe Map(group -> Stats(processed, affected))
    }
  }

  "finishing one device" should "work with several campaigns" in {
    val ns = arbitrary[Namespace].sample.get
    val group = NonEmptyList.one(GroupId.generate())
    val device = DeviceId.generate()

    for {
      update <- createDbUpdate(UpdateId.generate())
      newCampaigns <- FastFuture.traverse(arbitrary[Seq[Int]].sample.get)(_ => createDbCampaign(ns, update, group))
      _ <- FastFuture.traverse(newCampaigns)(c => campaigns.scheduleDevices(c.id, update, device))
      _ <- campaigns.finishDevice(update, device, DeviceStatus.successful)
      c <- campaigns.countFinished(newCampaigns.head.id)
    } yield c shouldBe 1
  }

  "finishing devices" should "work with one campaign" in {
    val devices  = arbitrary[Seq[DeviceId]].sample.get

    for {
      campaign <- createDbCampaignWithUpdate()
      _ <- FastFuture.traverse(devices)(d => campaigns.scheduleDevices(campaign.id, campaign.updateId, d))
      _ <- FastFuture.traverse(devices)(d => campaigns.finishDevice(campaign.updateId, d, DeviceStatus.failed))
      c <- campaigns.countFinished(campaign.id)
    } yield c shouldBe devices.length
  }
}
