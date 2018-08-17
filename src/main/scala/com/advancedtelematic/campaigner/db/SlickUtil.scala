package com.advancedtelematic.campaigner.db

import slick.dbio.DBIO
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Future


object SlickUtil {

  implicit class DBIOActionToFutureOps[T](value: DBIO[T]) {
    def run(implicit db: Database): Future[T] = db.run(value)
  }
}
