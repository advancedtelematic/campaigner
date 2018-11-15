package com.advancedtelematic.campaigner.daemon

import cats.syntax.show._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.{Campaigns, UpdateSupport}
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libats.data.DataType.{CampaignId => CampaignCorrelationId, MultiTargetUpdateId}
import com.advancedtelematic.libats.messaging_datatype.Messages.DeviceInstallationReport
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class DeviceInstallationReportListener()(implicit db: Database, ec: ExecutionContext)
  extends (DeviceInstallationReport => Future[Unit]) with UpdateSupport {

  private lazy val _log = LoggerFactory.getLogger(this.getClass)

  val campaigns = Campaigns()

  def apply(msg: DeviceInstallationReport): Future[Unit] = {
    val resultStatus = if (msg.result.success)
      DeviceStatus.successful
    else
      DeviceStatus.failed

    val f = msg.correlationId match {
      case CampaignCorrelationId(uuid) =>
        campaigns.finishDevices(CampaignId(uuid), Seq(msg.device), resultStatus)
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
}
