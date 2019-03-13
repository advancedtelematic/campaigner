package com.advancedtelematic.campaigner.actor

import java.time.Instant

import akka.Done
import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, Props}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink}
import com.advancedtelematic.campaigner.data.DataType.{CampaignId, CancelTaskStatus, DeviceStatus}
import com.advancedtelematic.campaigner.db._
import com.advancedtelematic.libats.data.DataType.{Namespace, CampaignId => CampaignCorrelationId}
import com.advancedtelematic.libats.messaging.MessageBusPublisher
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import com.advancedtelematic.libats.messaging_datatype.Messages.{DeviceUpdateCancelRequested, deviceUpdateEventCancelRequestedType}
import slick.jdbc.MySQLProfile.api.Database

import scala.concurrent.Future

object CampaignCanceler {
  private object Start

  def props(messageBus: MessageBusPublisher,
            campaign: CampaignId,
            ns: Namespace,
            batchSize: Int)
           (implicit db: Database, mat: Materializer): Props =
    Props(new CampaignCanceler(messageBus, campaign, ns, batchSize))
}

class CampaignCanceler(messageBus: MessageBusPublisher,
                       campaign: CampaignId,
                       ns: Namespace,
                       batchSize: Int)
                      (implicit db: Database, mat: Materializer)
    extends Actor
    with ActorLogging
    with GroupStatsSupport
    with CampaignSupport
    with DeviceUpdateSupport
    with CancelTaskSupport {

  import CampaignCanceler._
  import akka.pattern.pipe
  import context._

  override def preStart(): Unit =
    self ! Start

  private def cancel(deviceIds: Seq[DeviceId]): Future[Unit] = {
    val correlationId = CampaignCorrelationId(campaign.uuid)
    Future.traverse(deviceIds) { deviceId =>
      messageBus.publish(DeviceUpdateCancelRequested(ns, Instant.now, correlationId, deviceId))
    }.map(_ => ())
  }

  def run(): Future[Done] = for {
    _ <- campaignRepo.find(campaign)
    _ <- deviceUpdateRepo.findByCampaignStream(campaign, DeviceStatus.scheduled, DeviceStatus.accepted)
      .grouped(batchSize)
      .via(Flow[Seq[DeviceId]].mapAsync(1)(cancel))
      .runWith(Sink.ignore)
    _ <- cancelTaskRepo.setStatus(campaign, CancelTaskStatus.completed)
  } yield Done

  def receive: Receive = {
    case Start =>
      log.debug(s"Start to cancel devices for $campaign")
      run().recoverWith { case err =>
        cancelTaskRepo.setStatus(campaign, CancelTaskStatus.error).map(_ => Failure(err))
      }.pipeTo(self)
    case () =>
      log.debug(s"Done cancelling for $campaign")
      context.stop(self)
    case Failure(err) =>
      log.error(s"errors when cancelling $campaign", err)
      context.stop(self)
  }
}
