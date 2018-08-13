package com.advancedtelematic.campaigner.db

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Schema.{campaigns, updates}
import com.advancedtelematic.libats.messaging_datatype.DataType.UpdateId
import com.advancedtelematic.libats.slick.db.SlickExtensions._
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}


class ExtractUpdatesFromCampaignsAndInsert(implicit
                                           val db: Database,
                                           val mat: Materializer,
                                           val system: ActorSystem,
                                           val ec: ExecutionContext
                                   ) {

  private val _log = LoggerFactory.getLogger(this.getClass)

  private[db] def updateFromCampaign(c: Campaign): Update = {
    val u = Update(UpdateId.generate(), UpdateSource(c.updateId.uuid.toString, UpdateType.multi_target), c.namespace, c.name, None, Instant.now, Instant.now)
    _log.info("Extracted update {} from campaign {}", u, c, "")
    u
  }

  private[db] def createOneUpdateRecordForEachCampaign(): Future[Option[Int]] =
    db.run(campaigns.result)
      .map { cs => cs.map(updateFromCampaign) }
      .map { us => us.groupBy(_.source).map(_._2.head) } // filter UpdateSource duplicates
      .map { us => us.groupBy(u => (u.namespace, u.source.id)).map(_._2.head) } // filter unique key (namespace, updateId) duplicates (shouldn't be necessary after the previous filter?)
      .flatMap { us => db.run(updates ++= us) }

  def run() = {
    val campaignIdUpdateIdPairs = for {
      (c, u) <- campaigns join updates on (_.update.mappedTo[String] === _.updateId)
    } yield (c.id, u.uuid)

    createOneUpdateRecordForEachCampaign()
      .flatMap { _ => db.run(campaignIdUpdateIdPairs.result) }
      .flatMap { pairs => Future.traverse(pairs)(pair => db.run(campaigns.filter(_.id === pair._1).map(_.update).update(pair._2))) }
  }
}
