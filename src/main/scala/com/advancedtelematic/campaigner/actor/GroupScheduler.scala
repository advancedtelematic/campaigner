package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, ActorLogging, Props}
import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.CampaignSupport
import scala.concurrent.duration._
import slick.jdbc.MySQLProfile.api._
import scala.concurrent.Future

object GroupScheduler {

  private object NextBatch
  private final case class ScheduleBatch(offset: Long, processed: Long, affected: Long)
  final case class BatchComplete(group: GroupId, offset: Long)
  final case class GroupComplete(group: GroupId)
  private final case class Error(msg: String, error: Throwable)

  def props(registry: DeviceRegistryClient,
            director: DirectorClient,
            delay: FiniteDuration,
            batchSize: Long,
            campaign: Campaign,
            group: GroupId)
           (implicit db: Database): Props =
    Props(new GroupScheduler(registry, director, delay, batchSize, campaign, group))
}

class GroupScheduler(registry: DeviceRegistryClient,
                     director: DirectorClient,
                     delay: FiniteDuration,
                     batchSize: Long,
                     campaign: Campaign,
                     group: GroupId)
                    (implicit db: Database) extends Actor
  with ActorLogging
  with CampaignSupport {

  import GroupScheduler._
  import akka.pattern._
  import context._

  val scheduler = system.scheduler

  override def preStart() =
    self ! NextBatch

  private def resumeBatch(campaign: Campaign, group: GroupId): Future[ScheduleBatch] = for {
    offsetOpt <- Campaigns.remainingBatches(campaign.id, group)
    offset     = offsetOpt.getOrElse(0L)
    processed <- registry.devicesInGroup(campaign.namespace, group, offset, batchSize)
    affected  <- director.setMultiUpdateTarget(campaign.namespace, campaign.updateId, processed)
  } yield ScheduleBatch(offset, processed.length.toLong, affected.length.toLong)

  def receive: Receive = {
    case NextBatch =>
      resumeBatch(campaign, group)
        .recover { case err => Error("could not resume batch", err) }
        .pipeTo(self)
    case ScheduleBatch(_, processed, affected) if processed < batchSize =>
      log.debug(s"group $group complete")
      val stats = Stats(processed, affected)
      Campaigns.completeGroup(campaign.namespace, campaign.id, group, stats)
        .map(_ => GroupComplete(group))
        .recover { case err => Error("could not persist progress", err) }
        .pipeTo(parent)
        .andThen { case _ => context.stop(self) }
    case ScheduleBatch(offset, processed, affected) =>
      val frontier = offset + processed
      log.debug(s"batch complete for group $group from $processed to $frontier")
      scheduler.scheduleOnce(delay, self, NextBatch)
      Campaigns.completeBatch(campaign.namespace, campaign.id, group, Stats(frontier, affected))
        .map(_ => BatchComplete(group, frontier))
        .recover { case err => Error("could not complete batch", err) }
        .pipeTo(parent)
    case Error(msg, err) => log.error(s"$msg: ${err.getMessage}")

  }

}
