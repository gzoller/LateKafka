import sbt._
import Keys._

import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._
import bintray.BintrayKeys._

object Build extends Build {
  import Dependencies._

  lazy val basicSettings = Seq(
    organization                := "co.blocke",
    scalaVersion                := "2.11.8",
    version                     := "0.4.0",
    ScalariformKeys.preferences := ScalariformKeys.preferences.value
      .setPreference(AlignArguments, true)
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(PreserveDanglingCloseParenthesis, true),
    parallelExecution in Test   := false,
    scalacOptions in ThisBuild  ++= Seq("-Ywarn-unused-import", "-Xlint", "-feature", "-deprecation", "-encoding", "UTF8", "-unchecked")
  )

  val pubSettings = Seq (
    publishMavenStyle                    := true,
    bintrayOrganization                  := Some("blocke"),
    bintrayReleaseOnPublish in ThisBuild := false,
    licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
    bintrayRepository                    := "releases",
    bintrayPackageLabels                 := Seq("scala", "akka", "kafka")
  )

  lazy val root = Project(id = "latekafka", base = file("."))
    .settings(basicSettings: _*)
    .settings(pubSettings: _*)
    .settings(libraryDependencies ++=
      dep_compile(kafka_client,akka_stream,akka_slf4j) ++
      dep_test(scalajack,kafka,zkclient,scalatest)
  )
}
