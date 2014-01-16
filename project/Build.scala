import sbt._
import sbt.Keys._

object ProjectBuild extends Build {

  lazy val root = Project(
    id = "root",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "analytics",
      organization := "rakam.analytics",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.2",
      resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" % "akka-actor"      % "2.0.3",
        "com.typesafe.akka" % "akka-testkit"    % "2.0.3"  % "test",
        "junit"             % "junit"           % "4.5"             % "test",
        "org.scalatest"     % "scalatest_2.9.0" % "1.6.1"           % "test")
    )
  )
}
