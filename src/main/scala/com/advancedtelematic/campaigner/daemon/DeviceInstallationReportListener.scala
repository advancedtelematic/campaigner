package com.advancedtelematic.campaigner.daemon

import akka.http.scaladsl.util.FastFuture
import cats.syntax.show._
import com.advancedtelematic.campaigner.client.DeviceRegistryClient
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.{Campaigns, UpdateSupport}
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libats.data.DataType.{MultiTargetUpdateId, Namespace, CampaignId => CampaignCorrelationId}
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import com.advancedtelematic.libats.messaging_datatype.Messages.DeviceInstallationReport
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class DeviceInstallationReportListener(deviceRegistryClient: DeviceRegistryClient)(implicit db: Database, ec: ExecutionContext)
  extends (DeviceInstallationReport => Future[Unit]) with UpdateSupport {

  private lazy val _log = LoggerFactory.getLogger(this.getClass)

  val campaigns = Campaigns()

  def apply(msg: DeviceInstallationReport): Future[Unit] = {
    val success = msg.result.success
    val resultStatus = if (success) DeviceStatus.successful else DeviceStatus.failed

    val f = msg.correlationId match {
      case CampaignCorrelationId(uuid) =>
        val campaignId = CampaignId(uuid)
        val addDeviceToFailedDeviceGroupFuture =
          addDeviceToFailedDeviceGroup(msg.namespace, campaignId, msg.result.code, msg.device)
        for {
          _ <- if (success) Future.unit else addDeviceToFailedDeviceGroupFuture
          _ <- campaigns.finishDevices(CampaignId(uuid), Seq(msg.device), resultStatus)
        } yield ()
      case MultiTargetUpdateId(uuid) => for {
        update <- updateRepo.findByExternalId(msg.namespace, ExternalUpdateId(uuid.toString))
        _ <- campaigns.finishDevice(update.uuid, msg.device, resultStatus)
      } yield ()
    }

    f.recover {
      case Errors.MissingExternalUpdate(_) =>
        _log.info(s"Could not find an update with external id ${msg.correlationId}, ignoring message")
      case Errors.DeviceNotScheduled =>
        _log.info(s"Got DeviceUpdateReport for device ${msg.device.show} which is not scheduled by campaigner, ignoring this message.")
    }
  }

  private def addDeviceToFailedDeviceGroup(ns: Namespace, campaignId: CampaignId, failureCode: String, deviceId: DeviceId): Future[Unit] =
    findOrCreateFailedDeviceGroup(ns, campaignId, failureCode)
      .flatMap(gid => deviceRegistryClient.addDeviceToGroup(ns, gid, deviceId))

  private def findOrCreateFailedDeviceGroup(ns: Namespace, campaignId: CampaignId, failureCode: String): Future[GroupId] =
    campaigns.fetchFailedGroupId(campaignId, failureCode).flatMap {
      case Some(g) =>
        FastFuture.successful(g)
      case None =>
        deviceRegistryClient
          .createGroup(ns, s"failed-group-campaignId-$campaignId-failureCode-$failureCode")
          .flatMap(gid => campaigns.persistFailedGroup(campaignId, gid, failureCode))
    }

}
