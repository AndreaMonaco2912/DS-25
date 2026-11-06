ThisBuild / organization := "it.ds"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.9.0"

val pekkoVersion = "2.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "DS-25",

    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor"         % pekkoVersion,
      "org.apache.pekko" %% "pekko-slf4j"         % pekkoVersion,
      "ch.qos.logback"    % "logback-classic"     % "1.5.38",
      "org.apache.pekko" %% "pekko-testkit"       % pekkoVersion % Test,
      "com.github.sbt"    % "junit-interface"     % "0.13.3"     % Test
    ),

    javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),

    Compile / mainClass := Some("it.ds.Main"),

    // run in a separate JVM so Ctrl+C / System.exit and actor system shutdown behave
    run / fork := true,
    run / connectInput := true,
    Test / fork := true,
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v")
  )
