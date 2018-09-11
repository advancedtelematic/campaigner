package com.advancedtelematic.campaigner.daemon

import cats.syntax.show._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.{Campaigns, UpdateSupport}
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libtuf_server.data.Messages.DeviceUpdateReport
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class DeviceUpdateReportListener()(implicit db: Database, ec: ExecutionContext)
  extends (DeviceUpdateReport => Future[Unit]) with UpdateSupport {

  private lazy val _log = LoggerFactory.getLogger(this.getClass)

  val campaigns = Campaigns()

  def apply(msg: DeviceUpdateReport): Future[Unit] = {
    val status = if (msg.operationResult.values.forall(_.isSuccess))
      DeviceStatus.successful
    else
      DeviceStatus.failed

    val f = for {
      update <- updateRepo.findByExternalId(msg.namespace, ExternalUpdateId(msg.updateId.uuid.toString))
      _ <- campaigns.finishDevice(update.uuid, msg.device, status)
    } yield ()

    f.recover {
      case Errors.MissingExternalUpdate(_) =>
        _log.info(s"Could not find an update with external id ${msg.updateId.show}, ignoring message")
      case Errors.DeviceNotScheduled =>
        _log.info(s"Got DeviceUpdateReport for device ${msg.device.show} which is not scheduled by campaigner, ignoring this message.")
    }
  }
}
