package com.advancedtelematic.campaigner.http

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import akka.util.Timeout
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.actor.CampaignSupervisor
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.CampaignSupport
import com.advancedtelematic.libats.auth.AuthedNamespaceScope
import com.advancedtelematic.libats.data.Namespace
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import scala.concurrent.{ExecutionContext, Future}
import slick.jdbc.MySQLProfile.api._

class CampaignResource(extractAuth: Directive1[AuthedNamespaceScope],
                       supervisor: ActorRef)
                      (implicit db: Database, ec: ExecutionContext)
  extends CampaignSupport
  with Settings {

  import CampaignSupervisor._
  import akka.pattern.ask
  import scala.concurrent.duration._

  implicit val timeout = Timeout(10.seconds)

  def createCampaign(ns: Namespace, request: CreateCampaign): Future[CampaignId] = {
    val campaign = request.mkCampaign(ns)

    Campaigns.persist(campaign, request.groups)
      .map(_ => campaign.id)
  }

  def getCampaign(ns: Namespace, id: CampaignId): Future[GetCampaign] = for {
    c      <- Campaigns.findCampaign(ns, id)
    groups <- Campaigns.findGroups(ns, c.id)
  } yield GetCampaign(c, groups)

  def updateCampaign(ns: Namespace, id: CampaignId, updated: UpdateCampaign): Future[Unit] =
    Campaigns.update(ns, id, updated.name).map(_ => ())

  def launchCampaign(ns: Namespace, id: CampaignId): Future[Unit] =  for {
    c      <- Campaigns.findCampaign(ns, id)
    groups <- Campaigns.findGroups(ns, id)
    _      <- supervisor ? ScheduleCampaign(c, groups)
  } yield ()

  def getStats(ns: Namespace, id: CampaignId): Future[CampaignStatsResult] =
    Campaigns.campaignStatsFor(ns, id).map(CampaignStatsResult(id, _))

  val route =
    extractAuth { auth =>
      pathPrefix("campaigns") {
        val ns = auth.namespace
        (post & pathEnd) {
          entity(as[CreateCampaign]) { request =>
            complete(StatusCodes.Created -> createCampaign(ns, request))
          }
        } ~
        pathPrefix(CampaignId.Path) { id =>
          (get & pathEnd) {
            complete(getCampaign(ns, id))
          } ~
          (put & pathEnd & entity(as[UpdateCampaign])) { updated =>
            complete(updateCampaign(ns, id, updated))
          } ~
          (post & path("launch")) {
            complete(launchCampaign(ns, id))
          } ~
          (get & path("stats")) {
            complete(getStats(ns, id))
          }
        }
      }
    }

}
