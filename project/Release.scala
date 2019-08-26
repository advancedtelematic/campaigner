import com.typesafe.sbt.SbtNativePackager.Docker
import sbt.Keys._
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleaseStateTransformations.checkSnapshotDependencies

object Release {

  lazy val settings = {

    Seq(
      releaseProcess := Seq(
        checkSnapshotDependencies,
        ReleaseStep(releaseStepTask(publish in Docker))
      ),
      releaseIgnoreUntrackedFiles := true
    )
  }
}
