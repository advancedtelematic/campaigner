package com.advancedtelematic.campaigner.util

import akka.http.scaladsl.model.headers.RawHeader
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.test.LongTest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpecLike, Matchers}

trait CampaignerSpec extends FlatSpecLike
    with Matchers
    with ScalaFutures
    with LongTest
