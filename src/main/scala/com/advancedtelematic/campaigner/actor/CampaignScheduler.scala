package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, Props, Status}
import akka.stream.ActorMaterializer
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.{Campaigns, DeviceUpdateProcess}
import com.advancedtelematic.libats.messaging.MessageBusPublisher
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Future
import scala.concurrent.duration._

object CampaignScheduler {

  private final object NextBatch
  private final case class BatchToSchedule(devices: Set[DeviceId])
  private final object BatchComplete
  final case class CampaignComplete(campaign: CampaignId)

  def props(campaign: Campaign,
            delay: FiniteDuration,
            batchSize: Int)
           (implicit db: Database,
            messageBusPublisher: MessageBusPublisher): Props =
    Props(new CampaignScheduler(campaign, delay, batchSize))
}

class CampaignScheduler(campaign: Campaign,
                        delay: FiniteDuration,
                        batchSize: Int)
                       (implicit db: Database,
                        messageBusPublisher: MessageBusPublisher)
    extends Actor
    with ActorLogging {

  import CampaignScheduler._
  import akka.pattern.pipe
  import context._

  private val scheduler = system.scheduler
  private val campaigns = Campaigns()
  private val deviceUpdateProcess = new DeviceUpdateProcess()

  implicit val materializer = ActorMaterializer.create(context)

  override def preStart(): Unit =
    self ! NextBatch

  private def schedule(deviceIds: Set[DeviceId]): Future[Unit] =
    deviceUpdateProcess.startUpdateFor(campaign, deviceIds)

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
      schedule(devices).map(_ => BatchComplete).pipeTo(self)

    case BatchToSchedule(devices) if devices.isEmpty =>
      parent ! CampaignComplete(campaign.id)
      // TODO: Should move to finished
      context.stop(self)

    case BatchComplete =>
      log.debug(s"Completed a batch.")
      scheduler.scheduleOnce(delay, self, NextBatch)

    case Status.Failure(ex) =>
      log.error(ex, s"An Error occurred ${ex.getMessage}")
      throw ex
  }
}
