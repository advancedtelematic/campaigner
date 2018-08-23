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
                                           val ec: ExecutionContext) {

  private val _log = LoggerFactory.getLogger(this.getClass)

  private[db] def updateFromCampaign(c: Campaign): Update = {
    val u = Update(UpdateId.generate(), UpdateSource(ExternalUpdateId(c.updateId.uuid.toString), UpdateType.multi_target), c.namespace, c.name, None, Instant.now, Instant.now)
    _log.info("Extracted update {} from campaign {}", u.uuid, c.id, "")
    u
  }

  def run(): Future[Done.type] = db.run {
    campaigns.joinLeft(updates).on(_.update === _.uuid).filter(_._2.isEmpty).map(_._1)
      .result
      .map { cs => cs.groupBy(_.updateId) }
      .map { cs => cs.map { case (oldUpdateId, cc) => oldUpdateId -> updateFromCampaign(cc.head) } }
      .flatMap { us => (updates ++= us.values).map(_ => us) }
      .flatMap { us =>
        DBIO.sequence(
          us.foldLeft(List.empty[DBIO[Int]]) { case (acc, (oldUpdateId, update)) =>
            campaigns.filter(_.update === oldUpdateId).map(_.update).update(update.uuid) :: acc
          }
        ).map(_.sum)
      }
      .transactionally
      .map { result =>
        _log.info(s"created $result update rows")
        Done
      }
  }
}
