package com.advancedtelematic.campaigner.daemon

import cats.syntax.show._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.{Campaigns, UpdateSupport}
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libats.data.DataType.{CampaignId => CampaignCorrelationId, MultiTargetUpdateId}
import com.advancedtelematic.libats.messaging_datatype.DataType.UpdateId
import com.advancedtelematic.libats.messaging_datatype.Messages.{
  DeviceUpdateEvent,
  DeviceUpdateAssigned,
  DeviceUpdateCanceled,
  DeviceUpdateCompleted}
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class DeviceUpdateEventListener()(implicit db: Database, ec: ExecutionContext)
  extends (DeviceUpdateEvent => Future[Unit]) with UpdateSupport {

  private lazy val _log = LoggerFactory.getLogger(this.getClass)

  val campaigns = Campaigns()

  def apply(event: DeviceUpdateEvent): Future[Unit] = {
    event match {
      case _: DeviceUpdateAssigned => Future.successful(())
      case msg: DeviceUpdateCanceled  => handleUpdateCanceled(msg)
      case msg: DeviceUpdateCompleted => handleUpdateCompleted(msg)
      case _ => Future.successful(())
    }
  }

  def handleUpdateCanceled(msg: DeviceUpdateCanceled): Future[Unit] = {
      msg.correlationId match {
        case CampaignCorrelationId(uuid) =>
          campaigns.finishDevices(CampaignId(uuid), Seq(msg.deviceUuid), DeviceStatus.cancelled)
        case MultiTargetUpdateId(uuid) =>
          campaigns.findCampaignsByUpdate(UpdateId(uuid)).flatMap {
            case cs if cs.isEmpty =>
              _log.info(s"No campaign exists for ${msg.deviceUuid}")
              Future.successful(())
            case _ =>
              campaigns.finishDevice(UpdateId(uuid), msg.deviceUuid, DeviceStatus.cancelled)
          }
      }
  }

  def handleUpdateCompleted(msg: DeviceUpdateCompleted): Future[Unit] = {
    val resultStatus = if (msg.result.success)
      DeviceStatus.successful
    else
      DeviceStatus.failed

    val f = msg.correlationId match {
      case CampaignCorrelationId(uuid) =>
        campaigns.finishDevices(CampaignId(uuid), Seq(msg.deviceUuid), resultStatus)
      case MultiTargetUpdateId(uuid) => for {
        update <- updateRepo.findByExternalId(msg.namespace, ExternalUpdateId(uuid.toString))
        _ <- campaigns.finishDevice(update.uuid, msg.deviceUuid, resultStatus)
      } yield ()
    }

    f.recover {
      case Errors.MissingExternalUpdate(_) =>
        _log.info(s"Could not find an update with external id ${msg.correlationId}, ignoring message")
      case Errors.DeviceNotScheduled =>
        _log.info(s"Got DeviceUpdateEvent for device ${msg.deviceUuid.show} which is not scheduled by campaigner, ignoring this message.")
    }
  }
}
