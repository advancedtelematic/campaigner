name := "campaigner"
organization := "com.advancedtelematic"
scalaVersion in ThisBuild := "2.12.2"

scalacOptions := Seq(
  "-feature",
  "-unchecked",
  "-deprecation",
  "-encoding",
  "utf8",
  "-Xlint:_",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused-import"
  )

// allow imports in the console on a single line
scalacOptions in (Compile, console) ~= (_ filterNot (_ == "-Ywarn-unused-import"))

resolvers += "ATS Releases" at "http://nexus.advancedtelematic.com:8081/content/repositories/releases"

resolvers += "ATS Snapshots" at "http://nexus.advancedtelematic.com:8081/content/repositories/snapshots"

libraryDependencies ++= {
  val akkaHttpV = "10.0.3"
  val akkaV = "2.4.17"
  val libatsV = "0.0.1-93-g2c1a6db"
  val scalaTestV = "3.0.0"
  val slickV = "3.2.0"

  Seq(
    "ch.qos.logback" % "logback-classic" % "1.1.3",
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-http" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV,
    "com.typesafe.akka" %% "akka-slf4j" % akkaV,
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.slick" %% "slick" % slickV,
    "com.typesafe.slick" %% "slick-hikaricp" % slickV,
    "org.flywaydb" % "flyway-core" % "4.0.3",
    "org.mariadb.jdbc" % "mariadb-java-client" % "1.4.4",
    "org.typelevel" %% "cats" % "0.9.0",

    "com.advancedtelematic" %% "libats" % libatsV,
    "com.advancedtelematic" %% "libats-auth" % libatsV,
    "com.advancedtelematic" %% "libats-metrics" % libatsV,
    "com.advancedtelematic" %% "libats-metrics-akka" % libatsV,
    "com.advancedtelematic" %% "libats-messaging" % libatsV,
    "com.advancedtelematic" %% "libats-messaging-datatype" % libatsV,
    "com.advancedtelematic" %% "libats-slick" % libatsV,

    "org.scalacheck" %% "scalacheck" % "1.13.5" % Test,
    "org.scalatest" %% "scalatest" % scalaTestV % Test
  )
}

enablePlugins(BuildInfoPlugin)

buildInfoOptions += BuildInfoOption.ToMap

buildInfoOptions += BuildInfoOption.BuildTime

mainClass in Compile := Some("com.advancedtelematic.campaigner.Boot")

import com.typesafe.sbt.packager.docker._

dockerRepository in Docker := Some("advancedtelematic")

packageName in Docker := packageName.value

dockerUpdateLatest in Docker := true

defaultLinuxInstallLocation in Docker := s"/opt/${moduleName.value}"

dockerCommands := Seq(
  Cmd("FROM", "alpine:3.6"),
  Cmd("RUN", "apk upgrade --update && apk add --update openjdk8-jre bash coreutils"),
  ExecCmd("RUN", "mkdir", "-p", s"/var/log/${moduleName.value}"),
  Cmd("ADD", "opt /opt"),
  Cmd("WORKDIR", s"/opt/${moduleName.value}"),
  ExecCmd("ENTRYPOINT", s"/opt/${moduleName.value}/bin/${moduleName.value}"),
  Cmd("RUN", s"chown -R daemon:daemon /opt/${moduleName.value}"),
  Cmd("RUN", s"chown -R daemon:daemon /var/log/${moduleName.value}"),
  Cmd("USER", "daemon")
)

enablePlugins(JavaAppPackaging)

Revolver.settings

Versioning.settings

Release.settings

enablePlugins(Versioning.Plugin)
