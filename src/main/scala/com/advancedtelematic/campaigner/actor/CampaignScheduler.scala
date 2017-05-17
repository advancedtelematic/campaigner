package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, Props}
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.CampaignSupport
import slick.jdbc.MySQLProfile.api._
import scala.concurrent.duration._

object CampaignScheduler {

  private object NextGroup
  private final case class ScheduleGroup(group: GroupId)
  final case class CampaignComplete(campaign: CampaignId)
  private case class Error(msg: String, error: Throwable)

  def props(registry: DeviceRegistryClient,
            director: DirectorClient,
            campaign: Campaign,
            groups: Set[GroupId],
            delay: FiniteDuration,
            batchSize: Long)
           (implicit db: Database): Props =
    Props(new CampaignScheduler(registry, director, campaign, groups, delay, batchSize))

}

class CampaignScheduler(registry: DeviceRegistryClient,
                        director: DirectorClient,
                        campaign: Campaign,
                        groups: Set[GroupId],
                        delay: FiniteDuration,
                        batchSize: Long)
                       (implicit db: Database) extends Actor
  with ActorLogging
  with CampaignSupport {

  import CampaignScheduler._
  import GroupScheduler._
  import akka.pattern.pipe
  import context._

  override def preStart() =
    self ! NextGroup

  private def schedule(group: GroupId): Unit =
    actorOf(GroupScheduler.props(
      registry,
      director,
      delay,
      batchSize,
      campaign,
      group)
    )

  def receive: Receive = {
    case NextGroup =>
      Campaigns.remainingGroups(campaign.id)
        .map(_.headOption)
        .recover { case err => Error("could not retrieve remaining groups", err) }
        .pipeTo(self)
    case Some(group: GroupId) =>
      log.debug(s"scheduling group $group")
      schedule(group)
    case None =>
      parent ! CampaignComplete(campaign.id)
      context.stop(self)
    case GroupComplete(group) =>
      log.debug(s"group $group complete")
      self ! NextGroup
    case Error(msg, err) => log.error(s"$msg: ${err.getMessage}")
  }

}
