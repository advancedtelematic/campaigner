package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, Props}
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.CampaignSupport
import slick.jdbc.MySQLProfile.api._

object CampaignSupervisor {

  private final case class ResumeCampaigns(campaigns: Set[Campaign])
  final case class ScheduleCampaign(campaign: Campaign, groups: Set[GroupId])
  final case class CampaignScheduled(campaign: CampaignId)
  private final case class Error(msg: String, error: Throwable)

  def props(registry: DeviceRegistryClient,
            director: DirectorClient)
           (implicit db: Database): Props =
    Props(new CampaignSupervisor(registry, director))

}


class CampaignSupervisor(registry: DeviceRegistryClient, director: DirectorClient)
                        (implicit db: Database) extends Actor
  with ActorLogging
  with CampaignSupport {

  import CampaignSupervisor._
  import akka.pattern._
  import context._

  override def preStart() = {
    // re-schedule non-completed campaigns (pick up batches where they left)
    Campaigns
      .remainingCampaigns()
      .map(x => ResumeCampaigns(x.toSet))
      .recover { case err => Error("could not retrieve remaining campaigns", err) }
      .pipeTo(self)
  }

  def scheduleCampaign(campaign: Campaign, groups: Set[GroupId]): Unit =
    system.actorOf(CampaignScheduler.props(
      registry,
      director,
      campaign,
      groups
    ))

  def receive: Receive = {
    case ResumeCampaigns(campaigns) =>
      campaigns.foreach(scheduleCampaign(_, Set.empty))
    case ScheduleCampaign(campaign, groups) =>
      scheduleCampaign(campaign, groups)
      sender ! CampaignScheduled(campaign.id)
    case Error(msg, err) => log.error(s"$msg: ${err.getMessage}")
  }

}
