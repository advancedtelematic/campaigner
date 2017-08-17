package com.advancedtelematic.campaigner.daemon

import cats.syntax.show._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.campaigner.http.Errors
import com.advancedtelematic.libats.messaging_datatype.Messages.DeviceUpdateReport

import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import slick.jdbc.MySQLProfile.api._

object DeviceUpdateReportListener {
  private lazy val _log = LoggerFactory.getLogger(this.getClass)

  def apply(msg: DeviceUpdateReport)
           (implicit db: Database, ec: ExecutionContext): Future[Unit] =
    Campaigns().finishDevice(
      msg.updateId,
      msg.device,
      if (msg.operationResult.values.forall(_.isSuccess))
        DeviceStatus.successful
      else
        DeviceStatus.failed
    ).recover {
      case Errors.DeviceNotScheduled =>
        _log.info(s"Got DeviceUpdateReport for device ${msg.device.show} which is not scheduled by campaigner, ignoring this message.")
        ()
    }
}
