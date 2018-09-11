package com.advancedtelematic.campaigner.daemon

import java.util.UUID

import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, DeviceUpdateSupport, UpdateSupport}
import com.advancedtelematic.campaigner.util.{CampaignerSpec, DatabaseUpdateSpecUtil}
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import com.advancedtelematic.libats.test.DatabaseSpec
import com.advancedtelematic.libtuf_server.data.Messages.DeviceUpdateReport
import org.scalacheck.Arbitrary._

import scala.concurrent.ExecutionContext.Implicits.global

class DeviceUpdateReportListenerSpec extends CampaignerSpec with DatabaseSpec with DeviceUpdateSupport with UpdateSupport with DatabaseUpdateSpecUtil {
  val listener = new DeviceUpdateReportListener()

  val campaigns = Campaigns()

  "Listener" should "mark a device as successful using external update id" in {
    val updateSource = UpdateSource(ExternalUpdateId(UUID.randomUUID().toString), UpdateType.multi_target)
    val update = arbitrary[Update].gen.copy(source = updateSource)
    val campaign = arbitrary[Campaign].gen.copy(namespace = update.namespace, updateId = update.uuid)

    updateRepo.persist(update).futureValue
    campaigns.create(campaign, Set.empty, Seq.empty).futureValue

    val deviceUpdate = DeviceUpdate(campaign.id, update.uuid, DeviceId.generate(), DeviceStatus.accepted)

    deviceUpdateRepo.persistMany(Seq(deviceUpdate)).futureValue

    val report = DeviceUpdateReport(campaign.namespace, deviceUpdate.device, UpdateId(UUID.fromString(updateSource.id.value)), 0, Map.empty, 0)

    listener.apply(report).futureValue shouldBe (())

    deviceUpdateRepo.findByCampaign(campaign.id, DeviceStatus.successful).futureValue should contain(deviceUpdate.device)
  }
}
