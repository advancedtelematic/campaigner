package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.util.Timeout
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.db.CampaignSupport
import scala.util.{Failure, Success}
import slick.driver.MySQLDriver.api._

object Supervisor {

  def props(registry: DeviceRegistry,
            director: Director)
           (implicit db: Database): Props =
    Props(new Supervisor(registry, director))

}


class Supervisor(registry: DeviceRegistry, director: Director)
                (implicit db: Database) extends Actor
  with CampaignSupport {

  import CampaignScheduler._
  import StatsCollector._
  import akka.pattern._
  import context._
  import scala.concurrent.duration._

  val log = Logging(system, this)
  implicit val timeout: Timeout = 10.seconds

  override def preStart() = {
    val collector = system.actorOf(Props[StatsCollector])
    become(supervising(collector))
    collector ! StatsCollector.Start()

    // re-schedule non-completed campaigns (pick up batches where they left)
    val uncompleted = Campaigns.uncompletedCampaigns()
    uncompleted onComplete {
      case Success(m) =>
        m.foreach { case (campaign, grpOffsets) =>
          self ! ScheduleCampaign(campaign, grpOffsets)
        }
      case Failure(err) => log.error(err.toString)
    }

    // re-collect stats for completed and in-progress campaigns
    val all = Campaigns.campaignStats()
    all onComplete {
      case Success(m) =>
        m.foreach { case (campaign, grpStats) =>
          grpStats.foreach { case (grp, stats) =>
            collector ! Collect(campaign.id, grp, stats)
          }
        }
      case Failure(err) => log.error(err.toString)
    }
  }

  def supervising(collector: ActorRef): Receive = {
    case msg@Ask(_) =>
      val _ = collector ? msg pipeTo sender
    case ScheduleCampaign(campaign, grpOffsets) =>
      val actor = system.actorOf(CampaignScheduler.props(
                                  registry,
                                  director,
                                  collector,
                                  campaign
                                ))
      actor ! ScheduleCampaign(campaign, grpOffsets)
    case msg => log.info(s"unexpected message: $msg")
  }

  def receive: Receive = {
    case msg => log.info(s"unexpected message: $msg")
  }

}
