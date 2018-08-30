package com.advancedtelematic.campaigner.db

import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.data.DataType.{Campaign, CampaignId, DeviceStatus}
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class DeviceUpdateProcess(director: DirectorClient)(implicit db: Database, ec: ExecutionContext) {
  private val _logger = LoggerFactory.getLogger(this.getClass)

  val campaigns = Campaigns()

  def startUpdateFor(devices: Seq[DeviceId], campaign: Campaign): Future[Seq[DeviceId]] = {
    if(campaign.autoAccept)
      for {
        affected <- director.setMultiUpdateTarget(campaign.namespace, campaign.updateId, devices)
        _ <- campaigns.markDevicesAccepted(campaign.id, campaign.updateId, affected:_*)
      } yield affected
    else {
      for {
        affected <- director.findAffected(campaign.namespace, campaign.updateId, devices)
        _ <- campaigns.scheduleDevices(campaign.id, campaign.updateId, affected:_*)
      } yield affected
    }
  }

  def processDeviceAcceptedUpdate(ns: Namespace, campaignId: CampaignId, deviceId: DeviceId): Future[Unit] = {
    for {
      campaign <- campaigns.findClientCampaign(campaignId)
      affected <- director.setMultiUpdateTarget(ns, campaign.update, Seq(deviceId))
      _ <- affected.find(_ == deviceId) match {
        case Some(_) =>
          campaigns.markDevicesAccepted(campaignId, campaign.update, deviceId)
        case None =>
          _logger.warn(s"Could not start mtu update for device $deviceId after device accepted, device is no longer affected")

          campaigns.scheduleDevices(campaignId, campaign.update, deviceId).flatMap { _ =>
            campaigns.finishDevice(campaign.update, deviceId, DeviceStatus.failed)
          }
      }
    } yield ()
  }
}
