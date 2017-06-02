package com.advancedtelematic.campaigner.daemon

import akka.actor.{ActorSystem, Props}
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.CampaignSupport
import com.advancedtelematic.libats.messaging.MessageListener
import com.advancedtelematic.libats.messaging_datatype.Messages.DeviceUpdateReport
import com.typesafe.config.Config
import scala.concurrent.{ExecutionContext, Future}
import slick.jdbc.MySQLProfile.api._

object DeviceUpdateReportListener extends CampaignSupport {

  def handler(msg: DeviceUpdateReport)
    (implicit db: Database, ec: ExecutionContext): Future[Unit] =
    Campaigns.finishDevice(
      msg.updateId,
      msg.device,
      if (msg.operationResult.values.forall(_.isSuccess))
        DeviceStatus.successful
      else
        DeviceStatus.failed
    )

  def props(config: Config)
      (implicit db:Database, ec: ExecutionContext, system: ActorSystem): Props  =
    MessageListener.props[DeviceUpdateReport](config, handler)

}
