package com.advancedtelematic.campaigner.db

import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.data.DataType.{Campaign, CampaignId, DeviceStatus, UpdateType}
import com.advancedtelematic.libats.data.DataType.{CampaignId => CampaignCorrelationId, Namespace}
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class DeviceUpdateProcess(director: DirectorClient)(implicit db: Database, ec: ExecutionContext) extends UpdateSupport {

  private val _logger = LoggerFactory.getLogger(this.getClass)

  val campaigns = Campaigns()

  def startUpdateFor(devices: Seq[DeviceId], campaign: Campaign): Future[Seq[DeviceId]] = {
    updateRepo.findById(campaign.updateId).flatMap { update =>
      if (campaign.autoAccept)
        for {
          affected <- director.setMultiUpdateTarget(campaign.namespace,
                                                    update.source.id,
                                                    devices,
                                                    CampaignCorrelationId(campaign.id.uuid))
          _ <- campaigns.markDevicesAccepted(campaign.id, campaign.updateId, affected: _*)
        } yield affected
      else {
        for {
          affected <- if(update.source.sourceType == UpdateType.external) FastFuture.successful(devices)
                      else director.findAffected(campaign.namespace, update.source.id, devices)
          _ <- campaigns.scheduleDevices(campaign.id, campaign.updateId, affected: _*)
        } yield affected
      }
    }
  }

  def processDeviceAcceptedUpdate(ns: Namespace, campaignId: CampaignId, deviceId: DeviceId): Future[Unit] = {
    for {
      campaign <- campaigns.findClientCampaign(campaignId)
      update <- updateRepo.findById(campaign.update)
      affected <- director.setMultiUpdateTarget(ns, update.source.id, Seq(deviceId), CampaignCorrelationId(campaignId.uuid))
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
