package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Campaigns

import scala.concurrent.Future
import scala.concurrent.duration._
import slick.jdbc.MySQLProfile.api._

object CampaignSupervisor {

  private final object CleanUpCampaigns
  private final object PickUpCampaigns
  private final case class CancelCampaigns(campaigns: Set[CampaignId])
  private final case class ResumeCampaigns(campaigns: Set[Campaign])
  final case class ScheduleCampaign(campaign: Campaign)
  final case class CampaignsScheduled(campaigns: Set[CampaignId])
  final case class CampaignsCancelled(campaigns: Set[CampaignId])
  private final case class Error(msg: String, error: Throwable)

  def props(registry: DeviceRegistryClient,
            director: DirectorClient,
            pollingTimeout: FiniteDuration,
            delay: FiniteDuration,
            batchSize: Long)
           (implicit db: Database): Props =
    Props(new CampaignSupervisor(registry, director, pollingTimeout, delay, batchSize))

}

class CampaignSupervisor(registry: DeviceRegistryClient,
                         director: DirectorClient,
                         pollingTimeout: FiniteDuration,
                         delay: FiniteDuration,
                         batchSize: Long)
                        (implicit db: Database) extends Actor
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
  }

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
      Future.traverse(campaignSchedulers.keySet) { campaign =>
        campaigns.status(campaign).map { status => (campaign, status) }
      }
        .map(_.collect { case (campaign, CampaignStatus.cancelled) => campaign })
        .map(CancelCampaigns)
        .recover { case err => Error("could not get campaigns to be cancelled", err) }
        .pipeTo(self)
    case PickUpCampaigns =>
      campaigns
        .freshCampaigns()
        .map(x => ResumeCampaigns(x.toSet))
        .recover { case err => Error("could not get newly scheduled campaigns", err) }
        .pipeTo(self)
    case CancelCampaigns(campaigns) if campaigns.nonEmpty =>
      log.info(s"cancelling campaigns $campaigns")
      campaigns.foreach(c => stop(campaignSchedulers(c)))
      become(supervising(campaignSchedulers -- campaigns))
      parent ! CampaignsCancelled(campaigns)
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
      }
    case ScheduleCampaign(campaign) =>
      log.info(s"${campaign.id} scheduled")
      scheduleCampaign(campaign)
      parent ! CampaignsScheduled(Set(campaign.id))
    case CampaignComplete(id) =>
      log.info(s"$id completed")
      become(supervising(campaignSchedulers - id))
      parent ! CampaignComplete(id)
    case Error(msg, err) => log.error(s"$msg: ${err.getMessage}")
  }

  def receive: Receive = supervising(Map.empty)
}
