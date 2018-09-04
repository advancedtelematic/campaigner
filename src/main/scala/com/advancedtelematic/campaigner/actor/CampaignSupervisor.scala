package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.Materializer
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.libats.data.DataType.Namespace
import scala.concurrent.duration._
import slick.jdbc.MySQLProfile.api._

object CampaignSupervisor {

  private final object CleanUpCampaigns
  private final object PickUpCampaigns
  private final case class CancelCampaigns(campaigns: Set[(Namespace, CampaignId)])
  private final case class ResumeCampaigns(campaigns: Set[Campaign])

  // only for test
  final case class CampaignsScheduled(campaigns: Set[CampaignId])
  final case class CampaignsCancelled(campaigns: Set[CampaignId])

  private final case class Error(msg: String, error: Throwable)

  def props(registry: DeviceRegistryClient,
            director: DirectorClient,
            pollingTimeout: FiniteDuration,
            delay: FiniteDuration,
            batchSize: Long)
           (implicit db: Database, mat: Materializer): Props =
    Props(new CampaignSupervisor(registry, director, pollingTimeout, delay, batchSize))

}

class CampaignSupervisor(registry: DeviceRegistryClient,
                         director: DirectorClient,
                         pollingTimeout: FiniteDuration,
                         delay: FiniteDuration,
                         batchSize: Long)
                        (implicit db: Database, mat: Materializer) extends Actor
  with ActorLogging {

  import CampaignScheduler._
  import CampaignSupervisor._
  import akka.pattern.pipe
  import context._

  val scheduler = system.scheduler

  val campaigns = Campaigns()

  override def preStart(): Unit = {
    // periodically clear out cancelled campaigns
    scheduler.schedule(
      0.milliseconds,
      pollingTimeout,
      self,
      CleanUpCampaigns
    )

    // periodically (re-)schedule non-completed campaigns
    scheduler.schedule(
      0.milliseconds,
      pollingTimeout,
      self,
      PickUpCampaigns
    )

    // pick up campaigns where they left
    campaigns
      .remainingCampaigns()
      .map(x => ResumeCampaigns(x.toSet))
      .recover { case err => Error("could not retrieve remaining campaigns", err) }
      .pipeTo(self)

    // pick up cancelled campaigns where they left
    campaigns
      .remainingCancelling()
      .map(x => CancelCampaigns(x.toSet))
      .recover { case err => Error("could not retrieve remaining cancelling campaigns", err)}
      .pipeTo(self)
  }

  def cancelCampaign(ns: Namespace, campaign: CampaignId): ActorRef =
    context.actorOf(CampaignCanceler.props(
      director,
      campaign,
      ns,
      batchSize.toInt
    ))

  def scheduleCampaign(campaign: Campaign): ActorRef =
    context.actorOf(CampaignScheduler.props(
      registry,
      director,
      campaign,
      delay,
      batchSize
    ))

  def supervising(campaignSchedulers: Map[CampaignId, ActorRef]): Receive = {
    case CleanUpCampaigns =>
      log.debug(s"cleaning up campaigns")
      campaigns.freshCancelled()
        .map(x => CancelCampaigns(x.toSet))
        .recover { case err => Error("could not get campaigns to be cancelled", err) }
        .pipeTo(self)
    case PickUpCampaigns =>
      log.debug(s"picking up campaigns")
      campaigns
        .freshCampaigns()
        .map(x => ResumeCampaigns(x.toSet))
        .recover { case err => Error("could not get newly scheduled campaigns", err) }
        .pipeTo(self)
    case CancelCampaigns(campaigns) if campaigns.nonEmpty =>
      log.info(s"cancelling campaigns $campaigns")
      campaigns.foreach{case (_, c) => campaignSchedulers.get(c).foreach(stop)}
      campaigns.foreach{case (ns, c) => cancelCampaign(ns, c)}
      become(supervising(campaignSchedulers -- campaigns.map(_._2)))
      parent ! CampaignsCancelled(campaigns.map(_._2))
    case ResumeCampaigns(campaigns) if campaigns.nonEmpty =>
      log.info(s"resume campaigns ${campaigns.map(_.id)}")
      // only create schedulers for campaigns without a scheduler
      val newlyScheduled =
        campaigns
          .filterNot(c => campaignSchedulers.contains(c.id))
          .map(c => c.id -> scheduleCampaign(c))
          .toMap
      if (newlyScheduled.nonEmpty) {
        become(supervising(campaignSchedulers ++ newlyScheduled))
        parent ! CampaignsScheduled(newlyScheduled.keySet)
      } else
        log.debug(s"Not creating scheduler for campaigns, scheduler already exists")
    case CampaignComplete(id) =>
      log.info(s"$id completed")
      become(supervising(campaignSchedulers - id))
      parent ! CampaignComplete(id)
    case Error(msg, err) =>
      // TODO: Move campaign to failed?
      log.error(err, s"An error occurred: $msg")
  }

  def receive: Receive = supervising(Map.empty)
}
