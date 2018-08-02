package com.advancedtelematic.campaigner.db

import com.advancedtelematic.campaigner.data.DataType.Update
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.messaging_datatype.DataType.UpdateId
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

object Updates {
  def apply()(implicit db: Database, ec: ExecutionContext): Updates = new Updates()
}

protected[db] class Updates(implicit db: Database, ec: ExecutionContext) extends UpdateSupport  {

  def allUpdates(ns: Namespace, offset: Option[Long], limit: Option[Long]): Future[PaginationResult[UpdateId]] = {
    updateRepo.all(ns, offset.getOrElse(0L), limit.getOrElse(50L))
  }

  def create(update: Update) : Future[UpdateId] = db.run(updateRepo.persist(update))

}
