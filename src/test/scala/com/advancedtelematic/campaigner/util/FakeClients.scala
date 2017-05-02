package com.advancedtelematic.campaigner.client

import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.libats.data.Namespace
import java.util.concurrent.ConcurrentHashMap
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import scala.concurrent.{ExecutionContext, Future}

class FakeDeviceRegistryClient(implicit ec: ExecutionContext) extends DeviceRegistry {

  val state: ConcurrentHashMap[GroupId, Seq[DeviceId]] = new ConcurrentHashMap()

  override def getDevicesInGroup(namespace: Namespace,
                                 groupId: GroupId,
                                 offset: Int,
                                 limit: Int): Future[Seq[DeviceId]] =
    if (state.contains(groupId)) {
      FastFuture.successful(state.get(groupId).drop(offset).take(limit))
    } else {
      val r = arbitrary[Seq[DeviceId]].sample.get
      state.put(groupId, r)
      FastFuture.successful(r.drop(offset).take(limit))
    }

}

class FakeDirectorClient(implicit ec: ExecutionContext) extends Director {

  val state: ConcurrentHashMap[UpdateId, Seq[DeviceId]] = new ConcurrentHashMap()

  override def setMultiUpdateTarget(namespace: Namespace,
                                    update: UpdateId,
                                    devices: Seq[DeviceId]): Future[Seq[DeviceId]] = {
    val r = Gen.someOf(devices).sample.get
    state.put(update, r)
    FastFuture.successful(r)
  }

}
