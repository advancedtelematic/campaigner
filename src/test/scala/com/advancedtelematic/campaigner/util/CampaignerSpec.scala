package com.advancedtelematic.campaigner.util

import com.advancedtelematic.libats.test.LongTest
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpecLike, Matchers}

trait CampaignerSpecUtil {
  implicit class GenerateOps[T](value: Gen[T]) {
    def generate: T = value.pureApply(Gen.Parameters.default, Seed.random(), retries = 100)

    def gen = generate
  }
}

object CampaignerSpecUtil extends CampaignerSpecUtil

trait CampaignerSpec extends FlatSpecLike
  with Matchers
  with ScalaFutures
  with LongTest
  with CampaignerSpecUtil
