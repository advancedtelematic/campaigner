package com.advancedtelematic.campaigner.actor

import akka.Done
import akka.actor.{Actor, ActorLogging, Props}
import akka.actor.Status.Failure
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink}
import com.advancedtelematic.campaigner.db.{Campaigns, CampaignSupport, CancelTaskSupport, DeviceUpdateSupport, GroupStatsSupport}
import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.data.DataType.{CampaignId, CancelTaskStatus, DeviceStatus}
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import scala.concurrent.Future
import slick.jdbc.MySQLProfile.api.Database

object CampaignCanceler {
  private object Start

  def props(director: DirectorClient,
            campaign: CampaignId,
            ns: Namespace,
            batchSize: Int)
           (implicit db: Database, mat: Materializer): Props =
    Props(new CampaignCanceler(director, campaign, ns, batchSize))
}

class CampaignCanceler(director: DirectorClient,
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

  private val campaigns = Campaigns()

  override def preStart(): Unit =
    self ! Start

  private def cancel(devs: Seq[DeviceId]): Future[Seq[DeviceId]] =
    director.cancelUpdate(ns, devs).flatMap{ affected =>
      campaigns.cancelDevices(campaign, affected).map(_ => affected)
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
