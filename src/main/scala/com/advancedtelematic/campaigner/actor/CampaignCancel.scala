package com.advancedtelematic.campaigner.actor

import akka.Done
import akka.actor.{Actor, ActorLogging, Props}
import akka.actor.Status.Failure
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink}
import com.advancedtelematic.campaigner.db.{CampaignSupport, CancelTaskSupport, DeviceUpdateSupport}
import com.advancedtelematic.campaigner.data.DataType.{CampaignId, CancelTaskStatus, DeviceStatus}
import com.advancedtelematic.libats.data.DataType.{Namespace, CampaignId => CampaignCorrelationId}
import com.advancedtelematic.libats.messaging.MessageBusPublisher
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import com.advancedtelematic.libats.messaging_datatype.Messages._
import java.time.Instant
import scala.concurrent.Future
import slick.jdbc.MySQLProfile.api.Database

object CampaignCanceler {
  private object Start

  def props(campaignId: CampaignId,
            ns: Namespace,
            batchSize: Int)
           (implicit db: Database,
            mat: Materializer,
            messageBusPublisher: MessageBusPublisher): Props =
    Props(new CampaignCanceler(campaignId, ns, batchSize))
}

class CampaignCanceler(campaignId: CampaignId,
                       ns: Namespace,
                       batchSize: Int)
                      (implicit db: Database,
                       mat: Materializer,
                       messageBusPublisher: MessageBusPublisher)
    extends Actor
    with ActorLogging
    with CampaignSupport
    with DeviceUpdateSupport
    with CancelTaskSupport {

  import CampaignCanceler._
  import akka.pattern.pipe
  import context._

  override def preStart(): Unit =
    self ! Start

  private def cancel(deviceIds: Seq[DeviceId]): Future[Unit] =
    Future.traverse(deviceIds) { deviceId =>
      val msg: DeviceUpdateEvent = DeviceUpdateCancelRequested(
        ns,
        Instant.now(),
        CampaignCorrelationId(campaignId.uuid),
        deviceId)
      messageBusPublisher.publish(msg)
    }.map(_ => ())

  def run(): Future[Done] = for {
    _ <- campaignRepo.find(campaignId)
    _ <- deviceUpdateRepo.findByCampaignStream(campaignId, DeviceStatus.scheduled, DeviceStatus.accepted)
      .grouped(batchSize)
      .via(Flow[Seq[DeviceId]].mapAsync(1)(cancel))
      .runWith(Sink.ignore)
    _ <- cancelTaskRepo.setStatus(campaignId, CancelTaskStatus.completed)
  } yield Done

  def receive: Receive = {
    case Start =>
      log.debug(s"Start to cancel devices for $campaignId")
      run().recoverWith { case err =>
        cancelTaskRepo.setStatus(campaignId, CancelTaskStatus.error).map(_ => Failure(err))
      }.pipeTo(self)
    case () =>
      log.debug(s"Done cancelling for $campaignId")
      context.stop(self)
    case Failure(err) =>
      log.error(s"errors when cancelling $campaignId", err)
      context.stop(self)
  }
}
