package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.CampaignSupport
import slick.jdbc.MySQLProfile.api._

object CampaignSupervisor {

  private final object PickUpCampaigns
  private final case class ResumeCampaigns(campaigns: Set[Campaign])
  final case class ScheduleCampaign(campaign: Campaign, groups: Set[GroupId])
  final case class CampaignsScheduled(campaigns: Set[CampaignId])
  private final case class Error(msg: String, error: Throwable)

  def props(registry: DeviceRegistryClient,
            director: DirectorClient)
           (implicit db: Database): Props =
    Props(new CampaignSupervisor(registry, director))

}


class CampaignSupervisor(registry: DeviceRegistryClient, director: DirectorClient)
                        (implicit db: Database) extends Actor
  with ActorLogging
  with CampaignSupport
  with Settings {

  import CampaignScheduler._
  import CampaignSupervisor._
  import akka.pattern._
  import context._
  import scala.concurrent.duration._

  override def preStart() = {
    become(supervising(Map.empty))
    // periodically (re-)schedule non-completed campaigns
    system.scheduler.schedule(
        0.milliseconds,
        schedulerPollingTimeout,
        self,
        PickUpCampaigns
        )
    // pick up campaigns where they left
    Campaigns
      .remainingCampaigns()
      .map(x => ResumeCampaigns(x.toSet))
      .recover { case err => Error("could not retrieve remaining campaigns", err) }
      .pipeTo(self)
  }

  def scheduleCampaign(campaign: Campaign, groups: Set[GroupId]): ActorRef =
    system.actorOf(CampaignScheduler.props(
      registry,
      director,
      campaign,
      groups
    ))

  def supervising(campaignSchedulers: Map[CampaignId, ActorRef]): Receive = {
    case PickUpCampaigns =>
      Campaigns
        .freshCampaigns()
        .map(x => ResumeCampaigns(x.toSet))
        .recover { case err => Error("could not pick up campaigns to be scheduled", err) }
        .pipeTo(self)
    case ResumeCampaigns(campaigns) =>
      // only create schedulers for campaigns without a scheduler
      val newlyScheduled =
        campaigns
          .filter(c => !campaignSchedulers.keySet.contains(c.id))
          .map(c => c.id -> scheduleCampaign(c, Set.empty))
          .toMap
      log.debug(s"resume campaigns ${newlyScheduled.keySet}")
      become(supervising(campaignSchedulers ++ newlyScheduled))
      if (!newlyScheduled.keySet.isEmpty) {
        log.info(s"campaign scheduled: ${newlyScheduled.keySet}")
        parent ! CampaignsScheduled(newlyScheduled.keySet)
      }
    case ScheduleCampaign(campaign, groups) =>
      log.info(s"campaign scheduled: ${campaign.id}")
      scheduleCampaign(campaign, groups)
      sender ! CampaignsScheduled(Set(campaign.id))
    case CampaignComplete(id) =>
      become(supervising(campaignSchedulers - id))
    case Error(msg, err) => log.error(s"$msg: ${err.getMessage}")
  }

  def receive: Receive = Map.empty

}
