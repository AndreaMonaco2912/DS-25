ThisBuild / organization := "it.ds"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.9.0"

val pekkoVersion = "2.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "DS-25",

    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.20" % Test,
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.6",
      "ch.qos.logback" % "logback-classic" % "1.6.6"
    ),

    javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),

    Compile / mainClass := Some("it.ds.Main"),

    // run in a separate JVM so Ctrl+C / System.exit and actor system shutdown behave
    run / fork := true,
    run / connectInput := true,
    Test / fork := true,
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v")
  )
