package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.CampaignSupport
import scala.concurrent.ExecutionContext
import slick.driver.MySQLDriver.api._

object CampaignScheduler {

  private final case class LaunchCampaign()
  final case class ScheduleCampaign(campaign: Campaign, grpOffsets: Seq[(GroupId, Int)])
  final case class CampaignScheduled(campaign: CampaignId)
  final case class CampaignComplete(campaign: CampaignId)

  def props(registry: DeviceRegistry,
            director: Director,
            collector: ActorRef,
            campaign: Campaign)
           (implicit ec: ExecutionContext,
            db: Database): Props =
    Props(new CampaignScheduler(registry, director, collector, campaign))

}

class CampaignScheduler(registry: DeviceRegistry,
                        director: Director,
                        collector: ActorRef,
                        campaign: Campaign)
                       (implicit db: Database) extends Actor
  with CampaignSupport
  with Settings {

  import CampaignScheduler._
  import GroupScheduler._
  import StatsCollector._
  import context._

  val log = Logging(system, this)
  val scheduler = system.scheduler

  def schedule(grp: GroupId, offset: Int): Unit = {
    val actor = actorOf(GroupScheduler.props(
      registry,
      director,
      campaign.namespace,
      campaign.update,
      grp)
    )
    val _ = scheduler.scheduleOnce(schedulerDelay, actor, StartBatch(offset))
  }

  def scheduling(grpOffsets: Seq[(GroupId, Int)]): Receive = {
    case LaunchCampaign() => grpOffsets.headOption match {
      case Some((grp, offset)) =>
        log.debug(s"scheduling group $grp from $offset")
        schedule(grp, offset)
      case None =>
        log.debug(s"campaign ${campaign.update} completed")
        parent ! CampaignComplete(campaign.id)
        context stop self
    }
    case BatchComplete(grp, stats) =>
      log.debug(s"re-scheduling for group $grp")
      Campaigns.completeBatch(campaign.id, grp, stats)
      scheduler.scheduleOnce(schedulerDelay, sender, NextBatch())
      collector ! Collect(campaign.id, grp, stats)
    case GroupComplete(grp, stats) =>
      log.debug(s"group $grp complete with $stats")
      Campaigns.completeGroup(campaign.id, grp, stats)
      become(scheduling(grpOffsets.filter(_._1 == grp)))
      collector ! Collect(campaign.id, grp, stats)
      self ! LaunchCampaign()
    case msg => log.info(s"unexpected message: $msg")
  }

  def receive: Receive = {
    case ScheduleCampaign(c, grpOffsets) if c == campaign =>
      become(scheduling(grpOffsets))
      self   ! LaunchCampaign()
      sender ! CampaignScheduled(campaign.id)
    case msg => log.info(s"unexpected message: $msg")
  }

}
