package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directive1
import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.data.DataType.MetadataType.MetadataType
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.UserCampaignMetadataSupport
import com.advancedtelematic.libats.auth.AuthedNamespaceScope
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.http.MetadataResource.CreateUserMetadata
import io.circe.{Decoder, Encoder}

import scala.concurrent.ExecutionContext
import slick.jdbc.MySQLProfile.api._

object MetadataResource {
  import io.circe.generic.semiauto._

  case class CreateUserMetadata(`type`: MetadataType, value: String)

  implicit val createUserMetadataEncoder: Encoder[CreateUserMetadata] = deriveEncoder
  implicit val createUserMetadataDecoder: Decoder[CreateUserMetadata] = deriveDecoder
}

class MetadataResource(extractAuth: Directive1[AuthedNamespaceScope],
                       director: DirectorClient)
                      (implicit db: Database, ec: ExecutionContext) extends UserCampaignMetadataSupport {

  import akka.http.scaladsl.server.Directives._

  val route = extractAuth { ns =>
    path("metadata") {
      put {
        entity(as[CreateUserMetadata]) { req => // TODO: Overwrite, bump version
          val f = userCampaignMetadataRepo.persist(UserCampaignMetadata(ns.namespace, MetadataId.generate(), 0, req.`type`, req.value)).map(_ => StatusCodes.Created)
          complete(f)
        }
      } ~
      get {
        val f = userCampaignMetadataRepo.all(ns.namespace)
        complete(f)
      }
    }
  }
}