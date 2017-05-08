package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import scala.concurrent.ExecutionContext

object CampaignScheduler {

  private final case class LaunchCampaign()
  final case class ScheduleCampaign(grps: Set[GroupId])
  final case class CampaignScheduled(campaign: CampaignId)
  final case class CampaignComplete(campaign: CampaignId)

  def props(registry: DeviceRegistry,
            director: Director,
            collector: ActorRef,
            campaign: Campaign)
           (implicit ec: ExecutionContext): Props =
    Props(new CampaignScheduler(registry, director, collector, campaign))

}

class CampaignScheduler(registry: DeviceRegistry,
                        director: Director,
                        collector: ActorRef,
                        campaign: Campaign) extends Actor with Settings {

  import CampaignScheduler._
  import GroupScheduler._
  import StatsCollector._
  import context._

  val log = Logging(system, this)
  val scheduler = system.scheduler

  def schedule(grp: GroupId): Unit = {
    val actor = actorOf(GroupScheduler.props(
      registry,
      director,
      campaign.namespace,
      campaign.update,
      grp)
    )
    scheduler.scheduleOnce(schedulerDelay, actor, LaunchBatch())
    ()
  }

  def scheduling(grps: Set[GroupId]): Receive = {
    case LaunchCampaign() => grps.headOption match {
      case Some(grp) =>
        log.debug(s"scheduling group $grp")
        schedule(grp)
      case None =>
        log.debug(s"campaign ${campaign.update} completed")
        parent ! CampaignComplete(campaign.id)
        context stop self
    }
    case BatchComplete(grp, stats) =>
      log.debug(s"re-scheduling for group $grp")
      collector ! Collect(campaign.id, grp, stats)
      scheduler.scheduleOnce(schedulerDelay, sender, LaunchBatch())
      ()
    case GroupComplete(grp, stats) =>
      become(scheduling(grps - grp))
      collector ! Collect(campaign.id, grp, stats)
      self ! LaunchCampaign()
    case msg => log.info(s"unexpected message: $msg")
  }

  def receive: Receive = {
    case ScheduleCampaign(grps) =>
      become(scheduling(grps))
      self   ! LaunchCampaign()
      sender ! CampaignScheduled(campaign.id)
    case msg => log.info(s"unexpected message: $msg")
  }

}
