package com.advancedtelematic.campaigner.client

import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.libats.data.Namespace
import java.util.concurrent.ConcurrentHashMap
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class FakeDeviceRegistryClient(implicit ec: ExecutionContext) extends DeviceRegistry {

  val state: ConcurrentHashMap[GroupId, Seq[DeviceId]] = new ConcurrentHashMap()

  override def getDevicesInGroup(namespace: Namespace,
                                 grp: GroupId,
                                 offset: Int,
                                 limit: Int): Future[Seq[DeviceId]] =
    if (state.containsKey(grp)) {
      FastFuture.successful(state.get(grp).drop(offset).take(limit))
    } else {
      val r = arbitrary[Seq[DeviceId]].sample.get
      state.put(grp, r)
      FastFuture.successful(r.drop(offset).take(limit))
    }

}

class FakeDirectorClient(implicit ec: ExecutionContext) extends Director {

  val state: ConcurrentHashMap[UpdateId, Set[DeviceId]] = new ConcurrentHashMap()

  override def setMultiUpdateTarget(namespace: Namespace,
                                    update: UpdateId,
                                    devices: Seq[DeviceId]): Future[Seq[DeviceId]] = {
    val r = Gen.someOf(devices).sample.get
    val current = state.asScala.getOrElse(update, Set.empty)
    state.put(update, current ++ r)
    FastFuture.successful(r)
  }

}
