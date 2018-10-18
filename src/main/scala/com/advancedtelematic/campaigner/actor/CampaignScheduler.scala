package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, PoisonPill, Props, Status, SupervisorStrategy, Terminated}
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.{CampaignErrorsSupport, Campaigns}
import slick.jdbc.MySQLProfile.api._
import cats.syntax.option._

import scala.concurrent.duration._
import cats.syntax.show._

object CampaignScheduler {

  val MAX_CAMPAIGN_ERROR_COUNT = 10
  val MAX_DELAY = 5.minutes

  private object NextGroup
  private final case class ScheduleGroup(group: GroupId)
  final case class CampaignSchedulingComplete(campaign: CampaignId)
  final case class CampaignSchedulingFailed(campaignId: CampaignId)

  def props(registry: DeviceRegistryClient,
            director: DirectorClient,
            campaign: Campaign,
            delay: FiniteDuration,
            batchSize: Long)(implicit db: Database): Props =
    Props(new CampaignScheduler(registry, director, campaign, delay, batchSize))
}

class CampaignScheduler(registry: DeviceRegistryClient,
                        director: DirectorClient,
                        campaign: Campaign,
                        delay: FiniteDuration,
                        batchSize: Long)
                       (implicit db: Database) extends Actor
  with ActorLogging with CampaignErrorsSupport {

  import CampaignScheduler._
  import GroupScheduler._
  import akka.pattern.pipe
  import context._

  val campaigns = Campaigns()

  override def preStart(): Unit =
    self ! NextGroup

  private def startGroupScheduler(group: GroupId): Unit = {
    val props = GroupScheduler.props(registry, director, delay, batchSize, campaign, group)
    val child = actorOf(props, s"group-scheduler-${group.show}")
    context.watch(child)
  }

  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  private def calculateNextDelay(currentDelay: FiniteDuration)(implicit ord: Ordering[FiniteDuration]): FiniteDuration = {
    ord.min(currentDelay * 2, MAX_DELAY)
  }

  def scheduling(currentDelay: FiniteDuration, errorCount: Int, receivedError: Option[Throwable]): Receive = {
    case NextGroup =>
      log.debug(s"next group")
      campaigns.remainingGroups(campaign.id)
        .map(_.headOption)
        .pipeTo(self)

    case Some(group: GroupId) =>
      log.debug(s"scheduling $group")
      startGroupScheduler(group)

    case None =>
      parent ! CampaignSchedulingComplete(campaign.id)
      log.info(s"Scheduling of ${campaign.id} is complete")
      context.stop(self)

    case GroupSchedulerError(cause) =>
      become(scheduling(currentDelay, errorCount, cause.some))

    case Terminated(_) =>
      val nextDelay = calculateNextDelay(currentDelay)

      addCampaignError(receivedError)

      if(errorCount > MAX_CAMPAIGN_ERROR_COUNT) {
        log.warning(s"Giving up scheduling groups for ${campaign.id} after $errorCount tries")
        parent ! CampaignSchedulingFailed(campaign.id)
        context.stop(self)
      } else {
        log.info(s"Could not schedule groups for ${campaign.id} after $errorCount tries, trying again in $nextDelay")
        system.scheduler.scheduleOnce(nextDelay, self, NextGroup)
      }

      context.become(scheduling(nextDelay, errorCount + 1, receivedError = None))

    case GroupComplete(group) =>
      log.debug(s"$group complete")
      context.unwatch(sender)
      sender ! PoisonPill
      self ! NextGroup

    case Status.Failure(ex) =>
      addCampaignError(receivedError)
      log.error(ex, s"An error occurred scheduling a campaign ${ex.getMessage}")
      throw ex
  }

  private def addCampaignError(error: Option[Throwable]): Unit = {
    campaignErrorsRepo.addError(campaign.id, error.map(_.getMessage).getOrElse("Unknown error")).failed.foreach { ex =>
      log.error(ex, "Could not save campaign error to database")
    }
  }

  override def receive: Receive = scheduling(delay, errorCount = 0, receivedError = None)
}
