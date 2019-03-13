package com.advancedtelematic.campaigner.daemon

import java.time.Instant
import java.util.UUID

import cats.data.NonEmptyList
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, DeviceUpdateSupport, UpdateSupport}
import com.advancedtelematic.campaigner.util.{CampaignerSpec, DatabaseUpdateSpecUtil}
import com.advancedtelematic.libats.data.DataType.{CorrelationId, MultiTargetUpdateId, CampaignId => CampaignCorrelationId}
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, InstallationResult}
import com.advancedtelematic.libats.messaging_datatype.Messages.{
  DeviceUpdateEvent,
  DeviceUpdateCanceled,
  DeviceUpdateCompleted}
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalacheck.Arbitrary._

import scala.concurrent.ExecutionContext.Implicits.global

class DeviceUpdateEventListenerSpec extends CampaignerSpec
  with DatabaseSpec
  with DeviceUpdateSupport
  with UpdateSupport
  with DatabaseUpdateSpecUtil {

  val listener = new DeviceUpdateEventListener()

  val campaigns = Campaigns()

  "Listener" should "mark a device as successful using external update id" in {
    val (updateSource, campaign, deviceUpdate) = prepareTest()
    val report = makeReport(
      campaign,
      deviceUpdate,
      MultiTargetUpdateId(UUID.fromString(updateSource.id.value)),
      isSuccessful = true)

    listener.apply(report).futureValue shouldBe (())
    deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.successful).futureValue should contain(deviceUpdate.device)
  }

  "Listener" should "mark a device as successful using campaign CorrelationId" in {
    val (_, campaign, deviceUpdate) = prepareTest()
    val report = makeReport(
      campaign,
      deviceUpdate,
      CampaignCorrelationId(campaign.id.uuid),
      isSuccessful = true)

    listener.apply(report).futureValue shouldBe (())
    deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.successful).futureValue should contain(deviceUpdate.device)
  }

  "Listener" should "mark a device as failed using external update id" in {
    val (updateSource, campaign, deviceUpdate) = prepareTest()
    val report = makeReport(
      campaign,
      deviceUpdate,
      MultiTargetUpdateId(UUID.fromString(updateSource.id.value)),
      isSuccessful = false)

    listener.apply(report).futureValue shouldBe (())
    deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.failed).futureValue should contain(deviceUpdate.device)
    deviceUpdateRepo.findFailedByFailureCode(campaign.id, "FAILURE").futureValue should contain(deviceUpdate.device)
  }

  "Listener" should "mark a device as failed using campaign CorrelationId" in {
    val (_, campaign, deviceUpdate) = prepareTest()
    val report = makeReport(
      campaign,
      deviceUpdate,
      CampaignCorrelationId(campaign.id.uuid),
      isSuccessful = false)

    listener.apply(report).futureValue shouldBe (())
    deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.failed).futureValue should contain(deviceUpdate.device)
    deviceUpdateRepo.findFailedByFailureCode(campaign.id, "FAILURE").futureValue should contain(deviceUpdate.device)
  }

  "Listener" should "mark a device as canceled using campaign CorrelationId" in {
    val (_, campaign, deviceUpdate) = prepareTest()
    val event = DeviceUpdateCanceled(
      campaign.namespace,
      Instant.now,
      CampaignCorrelationId(campaign.id.uuid),
      deviceUpdate.device)

    listener.apply(event).futureValue shouldBe (())
    deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.cancelled).futureValue should contain(deviceUpdate.device)
  }

  private def prepareTest(): (UpdateSource, Campaign, DeviceUpdate) = {
    val updateSource = UpdateSource(ExternalUpdateId(UUID.randomUUID().toString), UpdateType.multi_target)
    val update = arbitrary[Update].generate.copy(source = updateSource)
    updateRepo.persist(update).futureValue

    val campaign = arbitrary[Campaign].generate.copy(namespace = update.namespace, updateId = update.uuid)
    val group = NonEmptyList.one(GroupId.generate())
    campaigns.create(campaign, group, Set.empty, Seq.empty).futureValue

    val deviceUpdate = DeviceUpdate(campaign.id, update.uuid, DeviceId.generate(), DeviceStatus.accepted)
    deviceUpdateRepo.persistMany(Seq(deviceUpdate)).futureValue

    (updateSource, campaign, deviceUpdate)
  }

  private def makeReport(
      campaign: Campaign,
      deviceUpdate: DeviceUpdate,
      correlationId: CorrelationId,
      isSuccessful: Boolean): DeviceUpdateEvent = {

    val installationResult =
      if (isSuccessful) {
        InstallationResult(true, "SUCCESS", "Successful update")
      } else {
        InstallationResult(false, "FAILURE", "Failed update")
      }

    DeviceUpdateCompleted(
      campaign.namespace,
      Instant.now,
      correlationId,
      deviceUpdate.device,
      installationResult,
      Map.empty,
      None)
  }
}
