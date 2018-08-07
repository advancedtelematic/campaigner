package com.advancedtelematic.campaigner.daemon

import akka.Done
import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.data.DataType.{CampaignId, DeviceStatus}
import com.advancedtelematic.campaigner.db.{CampaignSupport, Campaigns}
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, EventType}
import com.advancedtelematic.libats.messaging_datatype.Messages.DeviceEventMessage
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

object DeviceEventListener {
  import io.circe.generic.semiauto._
  import io.circe.{Decoder, Encoder}

  val CampaignAcceptedEventType = EventType("campaign_accepted", 0)

  case class AcceptedCampaign(campaignId: CampaignId)

  implicit val acceptedCampaignEncoder: Encoder[AcceptedCampaign] = deriveEncoder
  implicit val acceptedCampaignDecoder: Decoder[AcceptedCampaign] = deriveDecoder
}

class DeviceEventListener(directorClient: DirectorClient)(implicit db: Database, ec: ExecutionContext)
  extends (DeviceEventMessage => Future[Done]) with CampaignSupport {

  import DeviceEventListener._

  private val _logger = LoggerFactory.getLogger(this.getClass)

  val campaigns = Campaigns()

  private def startDeviceUpdate(ns: Namespace, campaignId: CampaignId, deviceId: DeviceId): Future[Unit] = {
    for {
      campaign <- campaigns.findCampaign(ns, campaignId)
      affected <- directorClient.setMultiUpdateTarget(ns, campaign.update, Seq(deviceId))
      _ <- affected.find(_ == deviceId) match {
        case Some(_) =>
          campaigns.markDeviceAccepted(campaignId, campaign.update, deviceId)
        case None =>
          _logger.warn(s"Could not start mtu update for device $deviceId after device accepted, device is no longer affected")

          campaigns.scheduleDevice(campaignId, campaign.update, deviceId).flatMap { _ =>
            campaigns.finishDevice(campaign.update, deviceId, DeviceStatus.failed)
          }
      }
    } yield ()
  }

  def apply(msg: DeviceEventMessage): Future[Done] =
    msg.event.eventType match {
      case CampaignAcceptedEventType =>
        for {
          acceptedCampaign <- Future.fromTry(msg.event.payload.as[AcceptedCampaign].toTry)
          _ <- startDeviceUpdate(msg.namespace, acceptedCampaign.campaignId, msg.event.deviceUuid)
        } yield Done

      case e @ EventType(CampaignAcceptedEventType.id, version) =>
        FastFuture.failed(new IllegalArgumentException(s"Could not process version $version of ${e.id} event: ${msg.event}"))

      case event =>
        _logger.debug(s"Ignoring unknown event $event from device ${msg.event.deviceUuid}")
        FastFuture.successful(Done)
    }
}
