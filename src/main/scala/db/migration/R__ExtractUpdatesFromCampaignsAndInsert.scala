package db.migration

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.advancedtelematic.campaigner.db.ExtractUpdatesFromCampaignsAndInsert
import com.advancedtelematic.libats.slick.db.AppMigration
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Future

class R__ExtractUpdatesFromCampaignsAndInsert extends AppMigration {

  implicit val system = ActorSystem(this.getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  override def migrate(implicit db: Database): Future[Unit] = new ExtractUpdatesFromCampaignsAndInsert().run().map(_ => ())

}
