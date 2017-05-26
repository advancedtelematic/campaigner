package com.advancedtelematic.campaigner.util

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, FlatSpec}

abstract class CampaignerSpec extends
       FlatSpec
  with Matchers
  with ScalaFutures
