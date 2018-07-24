package com.advancedtelematic.campaigner.http

import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.{CampaignMetadataSupport, CampaignSupport, DeviceUpdateSupport}
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class DeviceCampaigns()(implicit val ec: ExecutionContext, val db: Database) extends
  DeviceUpdateSupport with
  CampaignSupport with CampaignMetadataSupport  {

  def findScheduledCampaigns(deviceId: DeviceId): Future[GetDeviceCampaigns] = {
    deviceUpdateRepo.findDeviceCampaigns(deviceId, DeviceStatus.scheduled).map { campaigns =>
      val campaignWithMetadata = campaigns.groupBy(_._1).mapValues(_.flatMap(_._2))

      val clientCampaigns = campaignWithMetadata.map { case (campaign, meta) =>
        DeviceCampaign(campaign.id, campaign.name, meta.map(m => CreateCampaignMetadata(m.`type`, m.value)))
      }.toSeq

      GetDeviceCampaigns(deviceId, clientCampaigns)
    }
  }
}
