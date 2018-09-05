package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.client.{DeviceRegistryClient, ResolverClient}
import com.advancedtelematic.campaigner.data.DataType.{GroupId, Update}
import com.advancedtelematic.campaigner.db.UpdateSupport
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class GroupUpdateResolver(deviceRegistry: DeviceRegistryClient, resolver: ResolverClient, resolverUri: Uri)
                         (implicit db: Database, ec: ExecutionContext) extends UpdateSupport  {

  def groupUpdates(ns: Namespace, groupId: GroupId, maxRequests: Int = 50): Future[Seq[Update]] = {
    def fetchDevices(offset: Long, limit: Long, requests: Int = 1): Future[Seq[DeviceId]] = {
      deviceRegistry.devicesInGroup(ns, groupId, offset, limit).flatMap { devicesInBatch =>
        if (devicesInBatch.isEmpty)
          FastFuture.successful(devicesInBatch)
        else if (requests >= maxRequests)
          FastFuture.failed(Errors.TooManyRequestsToRemote)
        else
          fetchDevices(offset + devicesInBatch.size.toLong, limit, requests + 1).map { devicesInBatch ++ _}
      }
    }

    fetchDevices(offset = 0, limit = 50).flatMap { devices =>
      for {
        externalUpdates <- resolver.availableUpdatesFor(resolverUri, ns, devices)
        localUpdates <- updateRepo.findByExternalIds(ns, externalUpdates)
      } yield localUpdates
    }
  }
}
