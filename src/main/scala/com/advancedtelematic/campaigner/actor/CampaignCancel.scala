package com.advancedtelematic.campaigner.actor

import akka.Done
import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, Props}
import akka.http.scaladsl.util.FastFuture
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.data.DataType.DeviceStatus.DeviceStatus
import com.advancedtelematic.campaigner.data.DataType.{CampaignId, CancelTaskStatus, DeviceStatus}
import com.advancedtelematic.campaigner.db.{CampaignSupport, Campaigns, CancelTaskSupport, DeviceUpdateSupport}
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import slick.jdbc.MySQLProfile.api.Database

import scala.concurrent.Future

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
    with CampaignSupport
    with DeviceUpdateSupport
    with CancelTaskSupport {

  import CampaignCanceler._
  import akka.pattern.pipe
  import context._

  private val campaigns = Campaigns()

  override def preStart(): Unit =
    self ! Start

  private def cancel(devs: Seq[(DeviceId, DeviceStatus)]): Future[Done] = {
    val (requested, others)=  devs.toStream.partition(_._2 == DeviceStatus.requested)

    log.info(s"Canceling campaign requested: ${requested.size}, others: ${others.size}")

    val directorF =
      if(others.nonEmpty)
        director.cancelUpdate(ns, others.map(_._1))
      else
        FastFuture.successful(Seq.empty)

    directorF.flatMap{ affected =>
      log.info(s"cancelling ${affected.size} affected devices")
      campaigns.cancelDevices(campaign, affected)
    }.flatMap { _ =>
      log.info(s"cancelling ${requested.size} devices not yet scheduled")
      campaigns.cancelDevices(campaign, requested.map(_._1))
    }.map { _ =>
      Done
    }
  }

  def run(): Future[Done] = for {
    _ <- campaignRepo.find(campaign)
    _ <- deviceUpdateRepo.findByCampaignStream(campaign, DeviceStatus.scheduled, DeviceStatus.accepted, DeviceStatus.requested)
      .grouped(batchSize)
      .mapAsync(1)(cancel)
      .runWith(Sink.ignore)
    _ <- cancelTaskRepo.setStatus(campaign, CancelTaskStatus.completed)
  } yield Done

  def receive: Receive = {
    case Start =>
      log.debug(s"Start to cancel devices for $campaign")
      run().recoverWith { case err =>
        cancelTaskRepo.setStatus(campaign, CancelTaskStatus.error).map(_ => Failure(err))
      }.pipeTo(self)
    case Done =>
      log.debug(s"Done cancelling for $campaign")
      context.stop(self)
    case Failure(err) =>
      log.error(err, s"errors when cancelling $campaign")
      context.stop(self)
  }
}
