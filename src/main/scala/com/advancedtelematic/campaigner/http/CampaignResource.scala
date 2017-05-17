package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.CampaignSupport
import com.advancedtelematic.libats.auth.AuthedNamespaceScope
import com.advancedtelematic.libats.data.Namespace
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import scala.concurrent.{ExecutionContext, Future}
import slick.jdbc.MySQLProfile.api._

class CampaignResource(extractAuth: Directive1[AuthedNamespaceScope])
                      (implicit db: Database, ec: ExecutionContext)
  extends CampaignSupport
  with Settings {

  def createCampaign(ns: Namespace, request: CreateCampaign): Future[CampaignId] = {
    val campaign = request.mkCampaign(ns)
    Campaigns.persist(campaign, request.groups)
      .map(_ => campaign.id)
  }

  def getCampaign(ns: Namespace, id: CampaignId): Future[GetCampaign] = for {
    c      <- Campaigns.findCampaign(ns, id)
    groups <- Campaigns.findGroups(ns, c.id)
  } yield GetCampaign(c, groups)

  def launchCampaign(ns: Namespace, id: CampaignId): Future[Unit] =  for {
    groups <- Campaigns.findGroups(ns, id)
    ()     <- Campaigns.scheduleGroups(ns, id, groups)
  } yield ()

  def getStats(ns: Namespace, id: CampaignId): Future[CampaignStats] = for {
    status <- Campaigns.aggregatedStatus(id)
    stats  <- Campaigns.campaignStatsFor(ns, id)
  } yield CampaignStats(id, status, stats)

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
            complete(Campaigns.update(ns, id, updated.name).map(_ => ()))
          } ~
          (post & path("launch")) {
            complete(launchCampaign(ns, id))
          } ~
          (get & path("stats")) {
            complete(getStats(ns, id))
          } ~
          (post & path("cancel")) {
            complete(Campaigns.cancelCampaign(ns, id).map(_ => ()))
          }
        }
      }
    }

}
