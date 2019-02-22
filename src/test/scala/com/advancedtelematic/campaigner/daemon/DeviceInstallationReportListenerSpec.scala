package com.advancedtelematic.campaigner.daemon

import java.time.Instant
import java.util.UUID

import cats.data.NonEmptyList
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, DeviceUpdateSupport, FailedGroupSupport, UpdateSupport}
import com.advancedtelematic.campaigner.util.{CampaignerSpec, DatabaseUpdateSpecUtil, FakeDeviceRegistry}
import com.advancedtelematic.libats.data.DataType.{CorrelationId, MultiTargetUpdateId, Namespace, CampaignId => CampaignCorrelationId}
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, InstallationResult}
import com.advancedtelematic.libats.messaging_datatype.Messages.DeviceInstallationReport
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalacheck.Arbitrary._

import scala.concurrent.ExecutionContext.Implicits.global

class DeviceInstallationReportListenerSpec extends CampaignerSpec
  with DatabaseSpec
  with DeviceUpdateSupport
  with UpdateSupport
  with FailedGroupSupport
  with DatabaseUpdateSpecUtil {

  private val registry = new FakeDeviceRegistry
  val listener = new DeviceInstallationReportListener(registry)

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
    val (updateSource, campaign, deviceUpdate) = prepareTest()
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
  }

  "Listener" should "mark a device as failed using campaign CorrelationId" in {
    val (updateSource, campaign, deviceUpdate) = prepareTest()
    val report = makeReport(
      campaign,
      deviceUpdate,
      CampaignCorrelationId(campaign.id.uuid),
      isSuccessful = false)

    listener.apply(report).futureValue shouldBe (())
    deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.failed).futureValue should contain(deviceUpdate.device)
  }

  "Listener" should "create a failure group in device-registry and add a failed device to that group" in {
    val (_, campaign, deviceUpdate) = prepareTest()
    val report = makeReport(
      campaign,
      deviceUpdate,
      CampaignCorrelationId(campaign.id.uuid),
      isSuccessful = false)

    listener(report).futureValue
    val groupId = failedGroupRepo.fetchGroup(campaign.id, report.result.code).futureValue
    registry.devicesInGroup(Namespace("default"), groupId, 0, 10).futureValue should contain only report.device
  }

  "Listener" should "add a failed device to a failure group that already exists" in {
    val (_, campaign, deviceUpdate1) = prepareTest()
    val deviceUpdate2 = deviceUpdate1.copy(device = DeviceId.generate())
    val report1 = makeReport(
      campaign,
      deviceUpdate1,
      CampaignCorrelationId(campaign.id.uuid),
      isSuccessful = false)
    val report2 = makeReport(
      campaign,
      deviceUpdate2,
      CampaignCorrelationId(campaign.id.uuid),
      isSuccessful = false)

    listener(report1).futureValue
    listener(report2).futureValue
    val groupId = failedGroupRepo.fetchGroup(campaign.id, report2.result.code).futureValue
    registry.devicesInGroup(Namespace("default"), groupId, 0, 10).futureValue should contain only (report1.device, report2.device)

  }

  private def prepareTest(): (UpdateSource, Campaign, DeviceUpdate) = {
    val updateSource = UpdateSource(ExternalUpdateId(UUID.randomUUID().toString), UpdateType.multi_target)
    val update = arbitrary[Update].generate.copy(source = updateSource)
    updateRepo.persist(update).futureValue

    val campaign = arbitrary[Campaign].generate.copy(namespace = update.namespace, updateId = update.uuid)
    val group = NonEmptyList.one(GroupId.generate())
    campaigns.create(campaign, group, Seq.empty).futureValue

    val deviceUpdate = DeviceUpdate(campaign.id, update.uuid, DeviceId.generate(), DeviceStatus.accepted)
    deviceUpdateRepo.persistMany(Seq(deviceUpdate)).futureValue

    (updateSource, campaign, deviceUpdate)
  }

  private def makeReport(
      campaign: Campaign,
      deviceUpdate: DeviceUpdate,
      correlationId: CorrelationId,
      isSuccessful: Boolean): DeviceInstallationReport = {

    val installationResult =
      if (isSuccessful) {
        InstallationResult(true, "SUCCESS", "Successful update")
      } else {
        InstallationResult(false, "FAILURE", "Failed update")
      }

    DeviceInstallationReport(campaign.namespace,
      deviceUpdate.device,
      correlationId,
      installationResult,
      Map.empty,
      None,
      Instant.now)
  }
}
