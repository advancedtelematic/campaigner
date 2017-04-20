package com.advancedtelematic.util

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

abstract class CampaignerSpec extends
       PropSpec
  with Matchers
  with ScalaFutures
  with PropertyChecks
