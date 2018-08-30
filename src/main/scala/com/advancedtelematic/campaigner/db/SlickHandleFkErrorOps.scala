package com.advancedtelematic.campaigner.db
import java.sql.SQLIntegrityConstraintViolationException

import com.advancedtelematic.campaigner.http.Errors
import slick.dbio.DBIO
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

// TODO: Move to libats
protected[db] object SlickHandleFkErrorOps {
  implicit class SlickHandleFkErrorOps[T](value: DBIO[T])(implicit ec: ExecutionContext) {
    def handleForeignKeyError(throwable: Throwable): DBIO[T] = {
      value.asTry.flatMap {
        case Success(a) => DBIO.successful(a)
        case Failure(e: SQLIntegrityConstraintViolationException) if e.getErrorCode == 1452 => DBIO.failed(Errors.MissingUpdateSource)
        case Failure(e) => DBIO.failed(e)
      }
    }
  }
}
