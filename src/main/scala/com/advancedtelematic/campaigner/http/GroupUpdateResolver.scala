package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.client.{DeviceRegistryClient, ResolverClient}
import com.advancedtelematic.campaigner.data.DataType.{GroupId, Update}
import com.advancedtelematic.campaigner.db.UpdateSupport
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, TimeoutException}

class GroupUpdateResolver(deviceRegistry: DeviceRegistryClient, resolver: ResolverClient, resolverUri: Uri)
                         (implicit db: Database, ec: ExecutionContext) extends UpdateSupport  {

  def groupUpdates(ns: Namespace, groups: Set[GroupId], patience: Duration = 10.seconds): Future[Seq[Update]] = {

    val start = System.currentTimeMillis

    def fetchDevices(groupId: GroupId, offset: Long, limit: Long): Future[Seq[DeviceId]] = {

      deviceRegistry
        .devicesInGroup(ns, groupId, offset, limit)
        .flatMap { ds =>
          val spent = (System.currentTimeMillis - start).millis
          ds match {
            case Nil => FastFuture.successful(Seq.empty[DeviceId])
            case _ if patience - spent < 0.millis => FastFuture.failed(new TimeoutException)
            case _ => fetchDevices(groupId, offset + ds.length.toLong, limit).map(ds ++ _)
          }
        }
    }

    for {
      devices <- Future.sequence(groups.map(g => fetchDevices(g, offset = 0, limit = 50))).map(_.flatten)
      externalUpdates <- resolver.availableUpdatesFor(resolverUri, ns, devices)
      localUpdates <- updateRepo.findByExternalIds(ns, externalUpdates)
    } yield localUpdates

  }
}
