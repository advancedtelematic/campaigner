package com.advancedtelematic.campaigner.client

import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.libats.data.Namespace
import java.util.concurrent.ConcurrentHashMap
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import scala.concurrent.{ExecutionContext, Future}

class FakeDeviceRegistryClient(implicit ec: ExecutionContext) extends DeviceRegistryClient {

  val state: ConcurrentHashMap[GroupId, Seq[DeviceId]] = new ConcurrentHashMap()

  private implicit class IntOrException(n: Long) {
    def int: Int = if (n.isValidInt) {
      n.toInt
    } else {
      throw new IllegalArgumentException()
    }
  }

  override def devicesInGroup(namespace: Namespace,
                                 groupId: GroupId,
                                 offset: Long,
                                 limit: Long): Future[Seq[DeviceId]] =
    if (state.contains(groupId)) {
      FastFuture.successful(state.get(groupId).drop(offset.int).take(limit.int))
    } else {
      val r = arbitrary[Seq[DeviceId]].sample.get
      state.put(groupId, r)
      FastFuture.successful(r.drop(offset.int).take(limit.int))
    }

}

class FakeDirectorClient(implicit ec: ExecutionContext) extends DirectorClient {

  val state: ConcurrentHashMap[UpdateId, Seq[DeviceId]] = new ConcurrentHashMap()

  override def setMultiUpdateTarget(namespace: Namespace,
                                    update: UpdateId,
                                    devices: Seq[DeviceId]): Future[Seq[DeviceId]] = {
    val r = Gen.someOf(devices).sample.get
    state.put(update, r)
    FastFuture.successful(r)
  }

}
