package com.advancedtelematic.campaigner.client

import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.libats.data.Namespace
import java.util.concurrent.ConcurrentHashMap
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import scala.collection.JavaConverters._
import scala.concurrent.Future

class FakeDeviceRegistryClient extends DeviceRegistryClient {

  val state: ConcurrentHashMap[GroupId, Seq[DeviceId]] = new ConcurrentHashMap()

  override def devicesInGroup(namespace: Namespace,
                              groupId: GroupId,
                              offset: Long,
                              limit: Long): Future[Seq[DeviceId]] =
    if (state.containsKey(groupId)) {
      FastFuture.successful(state.get(groupId).drop(offset.toInt).take(limit.toInt))
    } else {
      val devices = arbitrary[Seq[DeviceId]].sample.get
      state.put(groupId, devices)
      FastFuture.successful(devices.drop(offset.toInt).take(limit.toInt))
    }

}

class FakeDirectorClient extends DirectorClient {

  val state: ConcurrentHashMap[UpdateId, Set[DeviceId]] = new ConcurrentHashMap()

  override def setMultiUpdateTarget(namespace: Namespace,
                                    update: UpdateId,
                                    devices: Seq[DeviceId]): Future[Seq[DeviceId]] = {
    val devs = Gen.someOf(devices).sample.get
    val current = state.asScala.getOrElse(update, Set.empty)
    state.put(update, current ++ devs)
    FastFuture.successful(devs)
  }

}
