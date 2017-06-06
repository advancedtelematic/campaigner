package com.advancedtelematic.campaigner.util

import com.advancedtelematic.libats.test.LongTest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, FlatSpecLike}

trait CampaignerSpec extends FlatSpecLike
    with Matchers
    with ScalaFutures
    with LongTest
