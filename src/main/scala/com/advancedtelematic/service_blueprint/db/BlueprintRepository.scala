package com.advancedtelematic.service_blueprint.db

import com.advancedtelematic.service_blueprint.http.Errors
import slick.driver.MySQLDriver.api._
import com.advancedtelematic.libats.http.Errors.MissingEntity

import scala.concurrent.{ExecutionContext, Future}

trait BlueprintRepositorySupport {
  def blueprintRepository(implicit db: Database, ec: ExecutionContext) = new BlueprintRepository()
}

protected class BlueprintRepository()(implicit db: Database, ec: ExecutionContext) {
  import com.advancedtelematic.data.DataType._
  import com.advancedtelematic.libats.db.SlickExtensions._
  import com.advancedtelematic.libats.db.Operators._

  val NotFound = MissingEntity(classOf[Blueprint])

  def persist(blueprint: Blueprint): Future[Unit] = {
    db.run {
      Schema.blueprints.insertOrUpdate(blueprint).map(_ => ()).handleIntegrityErrors(Errors.BlueprintMissing)
    }
  }

  def find(id: String): Future[Blueprint] = {
    db.run {
      Schema.blueprints
        .filter(_.id === id)
        .result.failIfNotSingle(NotFound)
    }
  }
}
