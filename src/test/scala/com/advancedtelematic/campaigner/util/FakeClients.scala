package com.advancedtelematic.campaigner.client

import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.libats.data.Namespace
import java.util.concurrent.ConcurrentHashMap
import org.scalacheck.Arbitrary._
import scala.concurrent.{ExecutionContext, Future}

class FakeDeviceRegistryClient(implicit ec: ExecutionContext) extends DeviceRegistry {

  val pseudoState: ConcurrentHashMap[GroupId, Seq[DeviceId]] = new ConcurrentHashMap()

  override def getDevicesInGroup(namespace: Namespace, groupId: GroupId): Future[Seq[DeviceId]] = {
    val r = arbitrary[Seq[DeviceId]].sample.get
    pseudoState.put(groupId, r)
    FastFuture.successful(r)
  }

}

class FakeDirectorClient(implicit ec: ExecutionContext) extends Director {

  val updatedDevices: ConcurrentHashMap[DeviceId, Unit] = new ConcurrentHashMap()

  override def setMultiUpdateTarget(namespace: Namespace,
                                    device: DeviceId,
                                    update: UpdateId): Future[Unit] = {
    updatedDevices.put(device, ())
    FastFuture.successful(())
  }

}
