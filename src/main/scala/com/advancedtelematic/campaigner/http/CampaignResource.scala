package com.advancedtelematic.campaigner.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.util.FastFuture
import akka.stream.Materializer
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.actor.CampaignScheduler
import com.advancedtelematic.campaigner.actor.CampaignScheduler._
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.CampaignSupport
import de.heikoseeberger.akkahttpcirce.CirceSupport._
import scala.concurrent.{ExecutionContext, Future}
import slick.driver.MySQLDriver.api._

class CampaignResource(registry: DeviceRegistry, director: Director)
  (implicit db: Database, ec: ExecutionContext, mat: Materializer, system: ActorSystem)
  extends CampaignSupport
  with Settings {

  def createCampaign(request: CreateCampaign): Future[CampaignId] = {
    val campaign = request.mkCampaign()

    Campaigns.persist(campaign, request.groups)
      .map(_ => campaign.id)
  }

  def getCampaign(id: CampaignId): Future[GetCampaign] = for {
    c    <- Campaigns.find(id)
    grps <- Campaigns.findGroups(c.id)
  } yield GetCampaign(c, grps)

  def updateCampaign(id: CampaignId, updated: UpdateCampaign): Future[Unit] =
    Campaigns.update(id, updated.name).map(_ => ())

  def launchCampaign(id: CampaignId): Future[Unit] =  for {
    c        <- Campaigns.find(id)
    grps     <- Campaigns.findGroups(id)
    _        <- {
      val actor = system.actorOf(CampaignScheduler.props(
        registry,
        director,
        schedulerDelay,
        schedulerBatchSize,
        c.namespace,
        c.update
      ))
      actor ! ScheduleCampaign(grps)
      FastFuture.successful(())
    }
  } yield ()

  val route =
    pathPrefix("campaigns") {
      (post & pathEnd) {
        entity(as[CreateCampaign]) { request =>
          complete(StatusCodes.Created -> createCampaign(request))
        }
      } ~
      pathPrefix(CampaignId.Path) { id =>
        (get & pathEnd) {
          complete(getCampaign(id))
        } ~
        (put & pathEnd & entity(as[UpdateCampaign])) { updated =>
          complete(updateCampaign(id, updated))
        } ~
        (post & path("launch")) {
          complete(launchCampaign(id))
        }
      }
    }

}
