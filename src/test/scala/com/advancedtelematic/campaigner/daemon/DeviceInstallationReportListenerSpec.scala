package com.advancedtelematic.campaigner.daemon

import java.time.Instant
import java.util.UUID

import cats.data.NonEmptyList
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, DeviceUpdateSupport, UpdateSupport}
import com.advancedtelematic.campaigner.util.{CampaignerSpec, DatabaseUpdateSpecUtil}
import com.advancedtelematic.libats.data.DataType.{CampaignId => CampaignCorrelationId, MultiTargetUpdateId}
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, InstallationResult}
import com.advancedtelematic.libats.messaging_datatype.Messages.DeviceInstallationReport
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalacheck.Arbitrary._

import scala.concurrent.ExecutionContext.Implicits.global

class DeviceInstallationReportListenerSpec extends CampaignerSpec
  with DatabaseSpec
  with DeviceUpdateSupport
  with UpdateSupport
  with DatabaseUpdateSpecUtil {

  val listener = new DeviceInstallationReportListener()

  val campaigns = Campaigns()

  "Listener" should "mark a device as successful using external update id" in {
    val updateSource = UpdateSource(ExternalUpdateId(UUID.randomUUID().toString), UpdateType.multi_target)
    val update = arbitrary[Update].gen.copy(source = updateSource)
    val campaign = arbitrary[Campaign].gen.copy(namespace = update.namespace, updateId = update.uuid)
    val group = NonEmptyList.one(GroupId.generate())

    updateRepo.persist(update).futureValue
    campaigns.create(campaign, group, Seq.empty).futureValue

    val deviceUpdate = DeviceUpdate(campaign.id, update.uuid, DeviceId.generate(), DeviceStatus.accepted)

    deviceUpdateRepo.persistMany(Seq(deviceUpdate)).futureValue

    val report = DeviceInstallationReport(campaign.namespace,
      deviceUpdate.device,
      MultiTargetUpdateId(UUID.fromString(updateSource.id.value)),
      InstallationResult(true, "SUCCESS", "Successful update"),
      Map.empty,
      None,
      Instant.now)

    listener.apply(report).futureValue shouldBe (())

    deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.successful).futureValue should contain(deviceUpdate.device)
  }

  "Listener" should "mark a device as successful using campaign CorrelationId" in {
    val updateSource = UpdateSource(ExternalUpdateId(UUID.randomUUID().toString), UpdateType.multi_target)
    val update = arbitrary[Update].gen.copy(source = updateSource)
    val campaign = arbitrary[Campaign].gen.copy(namespace = update.namespace, updateId = update.uuid)
    val group = NonEmptyList.one(GroupId.generate())

    updateRepo.persist(update).futureValue
    campaigns.create(campaign, group, Seq.empty).futureValue

    val deviceUpdate = DeviceUpdate(campaign.id, update.uuid, DeviceId.generate(), DeviceStatus.accepted)

    deviceUpdateRepo.persistMany(Seq(deviceUpdate)).futureValue

    val report = DeviceInstallationReport(campaign.namespace,
      deviceUpdate.device,
      CampaignCorrelationId(campaign.id.uuid),
      InstallationResult(true, "SUCCESS", "Successful update"),
      Map.empty,
      None,
      Instant.now)

    listener.apply(report).futureValue shouldBe (())

    deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.successful).futureValue should contain(deviceUpdate.device)
  }
}
