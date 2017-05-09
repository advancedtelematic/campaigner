package com.advancedtelematic.campaigner.actor

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.Namespace
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object CampaignScheduler {

  final case class LaunchCampaign()
  final case class ScheduleCampaign(grps: Set[GroupId])
  final case class CampaignComplete(update: UpdateId)

  def props(registry: DeviceRegistryClient,
            director: DirectorClient,
            delay: FiniteDuration,
            batchSize: Long,
            ns: Namespace,
            update: UpdateId)
           (implicit ec: ExecutionContext): Props =
    Props(new CampaignScheduler(registry, director, delay, batchSize, ns, update))

}

class CampaignScheduler(registry: DeviceRegistryClient,
                        director: DirectorClient,
                        delay: FiniteDuration,
                        batchSize: Long,
                        ns: Namespace,
                        update: UpdateId) extends Actor {

  import CampaignScheduler._
  import GroupScheduler._
  import context._

  val log = Logging(system, this)
  val scheduler = system.scheduler

  def schedule(grp: GroupId): Unit = {
    val actor = actorOf(GroupScheduler.props(registry, director, batchSize, ns, update, grp))
    scheduler.scheduleOnce(delay, actor, LaunchBatch())
    ()
  }

  def scheduling(grps: Set[GroupId]): Receive = {
    case LaunchCampaign() => grps.headOption match {
      case Some(grp) =>
        log.debug(s"scheduling group $grp")
        schedule(grp)
      case None =>
        log.debug(s"campaign $update completed")
        parent ! CampaignComplete(update)
    }
    case BatchComplete(grp, _) =>
      log.debug(s"re-scheduling $batchSize for group $grp")
      scheduler.scheduleOnce(delay, sender, LaunchBatch())
      ()
    case GroupComplete(grp) =>
      become(scheduling(grps - grp))
      self ! LaunchCampaign()
    case msg => log.info(s"unexpected message: $msg")
  }

  def receive: Receive = {
    case ScheduleCampaign(grps) =>
      become(scheduling(grps))
      self ! LaunchCampaign()
    case msg => log.info(s"unexpected message: $msg")
  }

}
