package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.stream.Materializer
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.libats.data.DataType.Namespace
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.duration._

object CampaignSupervisor {

  private final object CleanUpCampaigns
  private final object PickUpCampaigns
  private final case class CancelCampaigns(campaigns: Set[(Namespace, CampaignId)])
  private final case class ResumeCampaigns(campaigns: Set[Campaign])

  // only for test
  final case class CampaignsScheduled(campaigns: Set[CampaignId])
  final case class CampaignsCancelled(campaigns: Set[CampaignId])

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
    scheduler.schedule(0.milliseconds, pollingTimeout, self, CleanUpCampaigns)

    // pick up campaigns where they left
    campaigns
      .remainingCampaigns(CampaignScheduler.MAX_CAMPAIGN_ERROR_COUNT)
      .map(x => ResumeCampaigns(x.toSet))
      .pipeTo(self)

    // pick up cancelled campaigns where they left
    campaigns
      .remainingCancelling()
      .map(x => CancelCampaigns(x.toSet))
      .pipeTo(self)
  }

  def cancelCampaign(ns: Namespace, campaign: CampaignId): ActorRef =
    context.actorOf(CampaignCanceler.props(
      director,
      campaign,
      ns,
      batchSize.toInt
    ))

  def scheduleCampaign(campaign: Campaign): ActorRef = {
    val childProps = CampaignScheduler.props(registry, director, campaign, delay, batchSize)
    context.actorOf(childProps)
  }

  def supervising(campaignSchedulers: Map[CampaignId, ActorRef]): Receive = {
    case CleanUpCampaigns =>
      log.debug(s"cleaning up campaigns")
      campaigns.freshCancelled()
        .map(x => CancelCampaigns(x.toSet))
        .pipeTo(self)

    case PickUpCampaigns =>
      log.debug(s"picking up campaigns")
      campaigns
        .freshCampaigns(CampaignScheduler.MAX_CAMPAIGN_ERROR_COUNT)
        .map(x => ResumeCampaigns(x.toSet))
        .pipeTo(self)

    case CancelCampaigns(cs) if cs.nonEmpty =>
      log.info(s"cancelling campaigns ${cs.map(_._2)}")
      cs.foreach{case (_, c) => campaignSchedulers.get(c).foreach(stop)}
      cs.foreach{case (ns, c) => cancelCampaign(ns, c)}
      parent ! CampaignsCancelled(cs.map(_._2))
      become(supervising(campaignSchedulers -- cs.map(_._2)))

    case ResumeCampaigns(runningCampaigns) if runningCampaigns.nonEmpty =>
      log.info(s"resume campaigns ${runningCampaigns.map(_.id)}")

      // only create schedulers for campaigns without a scheduler
      val newlyScheduled =
        runningCampaigns
          .filterNot(c => campaignSchedulers.contains(c.id))
          .map(c => c.id -> scheduleCampaign(c))
          .toMap

      scheduler.scheduleOnce(pollingTimeout, self, PickUpCampaigns)

      if (newlyScheduled.nonEmpty) {
        parent ! CampaignsScheduled(newlyScheduled.keySet)
        become(supervising(campaignSchedulers ++ newlyScheduled))
      } else
        log.debug(s"Not creating scheduler for campaigns, scheduler already exists")

    case ResumeCampaigns(runningCampaigns) if runningCampaigns.isEmpty =>
      scheduler.scheduleOnce(pollingTimeout, self, PickUpCampaigns)

    case CampaignSchedulingFailed(id) =>
      // TODO Move campaign to failed when that state exists
      become(supervising(campaignSchedulers - id))

    case CampaignSchedulingComplete(id) =>
      log.info(s"$id scheduling supervision completed")
      parent ! CampaignSchedulingComplete(id)
      become(supervising(campaignSchedulers - id))

    case Status.Failure(ex) =>
      log.error(ex, s"An error occurred scheduling a campaign: ${ex.getMessage}")
  }

  def receive: Receive = supervising(Map.empty)
}
