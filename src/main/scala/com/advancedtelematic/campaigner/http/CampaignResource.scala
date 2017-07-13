package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.libats.auth.AuthedNamespaceScope
import com.advancedtelematic.libats.data.Namespace
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import scala.concurrent.{ExecutionContext, Future}
import slick.jdbc.MySQLProfile.api._

class CampaignResource(extractAuth: Directive1[AuthedNamespaceScope])
                      (implicit db: Database, ec: ExecutionContext)
  extends Settings {

  val campaigns = Campaigns()

  def createCampaign(ns: Namespace, request: CreateCampaign): Future[CampaignId] = {
    val campaign = request.mkCampaign(ns)
    campaigns.create(campaign, request.groups)
  }

  val route =
    extractAuth { auth =>
      pathPrefix("campaigns") {
        val ns = auth.namespace
        pathEnd {
          (get & parameters('limit.as[Long].?) & parameters('offset.as[Long].?)) { (mLimit, mOffset) =>
            val offset = mOffset.getOrElse(0L)
            val limit  = mLimit.getOrElse(50L)
            complete(campaigns.allCampaigns(ns, offset, limit))
          } ~
          (post & entity(as[CreateCampaign])) { request =>
            complete(StatusCodes.Created -> createCampaign(ns, request))
          }
        } ~
        pathPrefix(CampaignId.Path) { id =>
          pathEnd {
            get {
              complete(campaigns.findCampaign(ns, id))
            } ~
            (put & entity(as[UpdateCampaign])) { updated =>
              complete(campaigns.update(ns, id, updated.name))
            }
          } ~
          (post & path("launch")) {
            complete(campaigns.launch(ns, id))
          } ~
          (get & path("stats")) {
            complete(campaigns.campaignStats(ns, id))
          } ~
          (post & path("cancel")) {
            complete(campaigns.cancelCampaign(ns, id))
          }
        }
      }
    }
}
