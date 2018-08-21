package com.advancedtelematic.campaigner.db

import java.time.Instant

import akka.Done
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Schema.{campaigns, updates}
import com.advancedtelematic.libats.messaging_datatype.DataType.UpdateId
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

  def run(): Future[Done.type] = db.run {
    // This first line is a workaround for `campaigns.distinctOn(_.update).result` because there is a bug on `distinctOn`, see https://github.com/slick/slick/issues/1340.
    campaigns.result.map { cs => cs.groupBy(_.updateId).map(_._2.head) }
      .map { cs => cs.map(c => c.id -> updateFromCampaign(c)).toMap }
      .flatMap { us => (updates ++= us.values).map(_ => us) }
      .flatMap { us =>
        DBIO.sequence(
          us.foldLeft(List.empty[DBIO[Int]]) { case (acc, (campaignId, update)) =>
            campaigns.filter(_.id === campaignId).map(_.update).update(update.uuid) :: acc
          }
        )
      }
      .transactionally
      .map(_ => Done)
  }
}
