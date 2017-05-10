package com.advancedtelematic.campaigner.http

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import akka.util.Timeout
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.actor.CampaignScheduler
import com.advancedtelematic.campaigner.actor.StatsCollector
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.CampaignSupport
import de.heikoseeberger.akkahttpcirce.CirceSupport._
import scala.concurrent.{ExecutionContext, Future}
import slick.driver.MySQLDriver.api._

class CampaignResource(registry: DeviceRegistryClient, director: DirectorClient, collector: ActorRef)
                      (implicit db: Database, ec: ExecutionContext, mat: Materializer, system: ActorSystem)
  extends CampaignSupport
  with Settings {

  import CampaignScheduler._
  import StatsCollector._
  import akka.pattern.ask
  import scala.concurrent.duration._

  implicit val timeout = Timeout(10.seconds)

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
    c    <- Campaigns.find(id)
    grps <- Campaigns.findGroups(id)
    actor = system.actorOf(CampaignScheduler.props(
              registry,
              director,
              collector,
              c
            ))
    _ <- actor ? ScheduleCampaign(grps)
  } yield ()

  def getStats(id: CampaignId): Future[CampaignStatsResult] =
    ask(collector, Ask(id))
      .mapTo[CampaignStatsResult]

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
        } ~
        (get & path("stats")) {
          complete(getStats(id))
        }
      }
    }

}
