package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, Props, Status}
import akka.stream.ActorMaterializer
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.{Campaigns, DeviceUpdateProcess}
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success

object CampaignScheduler {

  private final object NextBatch
  private final case class BatchToSchedule(devices: Set[DeviceId])
  private final case class BatchComplete(affectedDevices: Set[DeviceId], rejectedDevices: Set[DeviceId])
  final case class CampaignComplete(campaign: CampaignId)

  def props(director: DirectorClient,
            campaign: Campaign,
            delay: FiniteDuration,
            batchSize: Int)
           (implicit db: Database): Props =
    Props(new CampaignScheduler(director, campaign, delay, batchSize))
}

class CampaignScheduler(director: DirectorClient,
                        campaign: Campaign,
                        delay: FiniteDuration,
                        batchSize: Int)
                       (implicit db: Database) extends Actor
  with ActorLogging {

  import CampaignScheduler._
  import DeviceUpdateProcess.StartUpdateResult
  import akka.pattern.pipe
  import context._

  private val scheduler = system.scheduler
  private val campaigns = Campaigns()
  private val deviceUpdateProcess = new DeviceUpdateProcess(director)

  implicit val materializer = ActorMaterializer.create(context)

  override def preStart(): Unit =
    self ! NextBatch

  private def schedule(deviceIds: Set[DeviceId]): Future[BatchComplete] = for {
    StartUpdateResult(accepted, scheduled, rejected) <- deviceUpdateProcess.startUpdateFor(deviceIds, campaign)
    _ <- campaigns.updateCampaignAndDevicesStatuses(campaign, accepted, scheduled, rejected)
  } yield BatchComplete(accepted ++ scheduled, rejected)

  def receive: Receive = {
    case NextBatch =>
      log.debug("Requesting next batch")
      campaigns.requestedDevicesStream(campaign.id)
        .take(batchSize.toLong)
        .runFold(Set.empty[DeviceId])(_ + _)
        .map(BatchToSchedule)
        .pipeTo(self)

    case BatchToSchedule(devices) if devices.nonEmpty =>
      log.debug(s"Scheduling new batch. Size: ${devices.size}.")
      schedule(devices).pipeTo(self)

    case BatchToSchedule(devices) if devices.isEmpty =>
      campaigns
        .updateStatus(campaign.id)
        .transform(_ => Success(CampaignComplete(campaign.id)))
        .pipeTo(self)

    case BatchComplete(affectedDevices, rejectedDevices) =>
      log.debug(s"Completed a batch. Affected: ${affectedDevices.size}. Rejected: ${rejectedDevices.size}.")
      scheduler.scheduleOnce(delay, self, NextBatch)

    case msg @ CampaignComplete(campaignId) =>
      log.debug(s"Completed campaign: $campaignId")
      parent ! msg
      context.stop(self)

    case Status.Failure(ex) =>
      log.error(ex, s"An Error occurred ${ex.getMessage}")
      throw ex
  }
}
