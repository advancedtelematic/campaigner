package com.advancedtelematic.campaigner.db

import java.time.Instant

import com.advancedtelematic.campaigner.data.DataType.{Campaign, CampaignId, Update}
import com.advancedtelematic.libats.data.DataType.{CampaignId => CampaignCorrelationId}
import com.advancedtelematic.libats.messaging.MessageBusPublisher
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, SourceUpdateId}
import com.advancedtelematic.libats.messaging_datatype.Messages._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class DeviceUpdateProcess()(
    implicit db: Database,
    ec: ExecutionContext,
    messageBusPublisher: MessageBusPublisher)
    extends UpdateSupport
    with CampaignSupport {

  val campaigns = Campaigns()

  /**
   * For a given campaign and a set of devices, sets statuses of these devices
   * in the campaign to `scheduled` and update the overall status of the
   * campaign. Additionally, if the campaign is automatically accepted,
   * publishes an update assignment request message for each device.
   */
  def startUpdateFor(campaign: Campaign, devices: Set[DeviceId]): Future[Unit] =
    updateRepo.findById(campaign.updateId).flatMap { update =>
      for {
        _ <- campaigns.scheduleDevices(campaign.id, devices.toSeq)
        _ <- if (campaign.autoAccept) requestDevicesUpdateAssignment(campaign, update, devices) else Future.successful(())
        _ <- campaigns.updateStatus(campaign.id)
      } yield ()
    }

  /**
   * For given campaign and device publishes an update assignment request
   * message
   */
  def processDeviceAcceptedUpdate(campaignId: CampaignId, deviceId: DeviceId): Future[Unit] = for {
    campaign <- campaignRepo.find(campaignId)
    update <- updateRepo.findById(campaign.updateId)
    _ <- requestUpdateAssignment(campaign, update, deviceId)
  } yield ()

  private def requestDevicesUpdateAssignment(campaign: Campaign, update: Update, devices: Set[DeviceId]): Future[Unit] =
    Future.traverse(devices)(deviceId => requestUpdateAssignment(campaign, update, deviceId)).map(_ => ())

  private def requestUpdateAssignment(campaign: Campaign, update: Update, deviceId: DeviceId): Future[Unit] =
    messageBusPublisher.publish(DeviceUpdateAssignmentRequested(
      campaign.namespace,
      Instant.now(),
      CampaignCorrelationId(campaign.id.uuid),
      deviceId,
      SourceUpdateId(update.source.id.value)
    ).asInstanceOf[DeviceUpdateEvent])
}
