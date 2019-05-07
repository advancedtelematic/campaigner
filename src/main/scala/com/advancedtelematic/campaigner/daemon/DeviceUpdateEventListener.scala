package com.advancedtelematic.campaigner.daemon

import cats.syntax.show._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.{Campaigns, UpdateSupport}
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libats.data.DataType.{MultiTargetUpdateId, CampaignId => CampaignCorrelationId}
import com.advancedtelematic.libats.messaging_datatype.DataType.UpdateId
import com.advancedtelematic.libats.messaging_datatype.Messages.{DeviceUpdateCanceled, DeviceUpdateCompleted, DeviceUpdateEvent}
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class DeviceUpdateEventListener()(implicit db: Database, ec: ExecutionContext)
  extends (DeviceUpdateEvent => Future[Unit]) with UpdateSupport {

  private lazy val _log = LoggerFactory.getLogger(this.getClass)

  val campaigns = Campaigns()

  def apply(event: DeviceUpdateEvent): Future[Unit] = {
    event match {
      case msg: DeviceUpdateCanceled  => handleUpdateCanceled(msg)
      case msg: DeviceUpdateCompleted => handleUpdateCompleted(msg)
      case _ => Future.successful(())
    }
  }

  def handleUpdateCanceled(msg: DeviceUpdateCanceled): Future[Unit] = {
      msg.correlationId match {
        case CampaignCorrelationId(uuid) =>
          campaigns.cancelDevices(CampaignId(uuid), Seq(msg.deviceUuid))
        case MultiTargetUpdateId(uuid) =>
          campaigns.findCampaignsByUpdate(UpdateId(uuid)).flatMap {
            case cs if cs.isEmpty =>
              _log.info(s"No campaign exists for ${msg.deviceUuid}")
              Future.successful(())
            case _ =>
              campaigns.cancelDevice(UpdateId(uuid), msg.deviceUuid)
          }
      }
  }

  def handleUpdateCompleted(msg: DeviceUpdateCompleted): Future[Unit] = {
    val resultCode = msg.result.code
    val resultDescription = msg.result.description

    val f = (msg.result.success, msg.correlationId) match {

      case (true, CampaignCorrelationId(uuid)) =>
        campaigns.succeedDevices(CampaignId(uuid), Seq(msg.deviceUuid), resultCode, resultDescription)

      case (false, CampaignCorrelationId(uuid)) =>
        campaigns.failDevices(CampaignId(uuid), Seq(msg.deviceUuid), resultCode, resultDescription)

      case (true, MultiTargetUpdateId(uuid)) =>
        for {
          update <- updateRepo.findByExternalId(msg.namespace, ExternalUpdateId(uuid.toString))
          _ <- campaigns.succeedDevice(update.uuid, msg.deviceUuid, resultCode, resultDescription)
        } yield ()

      case (false, MultiTargetUpdateId(uuid)) =>
        for {
          update <- updateRepo.findByExternalId(msg.namespace, ExternalUpdateId(uuid.toString))
          _ <- campaigns.failDevice(update.uuid, msg.deviceUuid, resultCode, resultDescription)
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
