ThisBuild / organization := "ds25"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.9.0"

// pekko-http 1.4.x (latest stable) is built against Pekko 1.x
val pekkoVersion     = "1.7.1"
val pekkoHttpVersion = "1.4.2"

lazy val commonSettings = Seq(
  // Docker images run on JRE 21
  scalacOptions ++= Seq("-release", "21", "-deprecation", "-feature", "-unchecked"),
  javacOptions ++= Seq("--release", "21"),
  libraryDependencies ++= Seq(
    "org.apache.pekko"           %% "pekko-actor-typed"         % pekkoVersion,
    "org.apache.pekko"           %% "pekko-slf4j"               % pekkoVersion,
    "com.typesafe.scala-logging" %% "scala-logging"             % "3.9.6",
    "ch.qos.logback"              % "logback-classic"           % "1.6.6",
    "org.apache.pekko"           %% "pekko-actor-testkit-typed" % pekkoVersion % Test,
    "org.scalatest"              %% "scalatest"                 % "3.2.20"     % Test
  ),
  run / fork  := true,
  Test / fork := true
)

lazy val dockerSettings = Seq(
  dockerBaseImage    := "eclipse-temurin:21-jre",
  dockerExposedPorts := Seq(8080, 25520),
  dockerUpdateLatest := true,
  // Containers get new IPs when restarted: don't let the JVM cache name lookups (default 30s).
  bashScriptExtraDefines ++= Seq(
    """addJava "-Dsun.net.inetaddr.ttl=2"""",
    """addJava "-Dsun.net.inetaddr.negative.ttl=0""""
  )
)

lazy val root = (project in file("."))
  .aggregate(common, tracker, client)
  .settings(name := "ds25", publish / skip := true)

/** Messages and JSON formats shared by trackers and clients. */
lazy val common = (project in file("common"))
  .settings(commonSettings)
  .settings(
    name := "ds25-common",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-serialization-jackson" % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream"                % pekkoVersion,
      "org.apache.pekko" %% "pekko-http-spray-json"       % pekkoHttpVersion
    )
  )

/** Tracker node: HTTP /announce API + Pekko Cluster with a replicated swarm registry. */
lazy val tracker = (project in file("tracker"))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(common)
  .settings(commonSettings, dockerSettings)
  .settings(
    name                := "ds25-tracker",
    Compile / mainClass := Some("ds25.tracker.TrackerMain"),
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-cluster-typed"    % pekkoVersion,
      "org.apache.pekko" %% "pekko-distributed-data" % pekkoVersion,
      "org.apache.pekko" %% "pekko-http"             % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-stream-testkit"   % pekkoVersion     % Test,
      "org.apache.pekko" %% "pekko-http-testkit"     % pekkoHttpVersion % Test
    )
  )

/** Peer: announces to the tracker over HTTP, talks to other peers over Artery remoting. */
lazy val client = (project in file("client"))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(common)
  .settings(commonSettings, dockerSettings)
  .settings(
    name                := "ds25-client",
    Compile / mainClass := Some("ds25.client.ClientMain"),
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-remote" % pekkoVersion,
      "org.apache.pekko" %% "pekko-http"   % pekkoHttpVersion
    )
  )
