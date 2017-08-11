name := """elasticSearchStream"""
organization := "adc.tutorial.elasticsearch"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.12.2"

libraryDependencies += guice
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.0" % Test
libraryDependencies += "com.typesafe.play" %% "play-ahc-ws-standalone" % "1.0.1"
libraryDependencies += "com.typesafe.play" %% "play-ws-standalone-json" % "1.0.1"
libraryDependencies += "com.typesafe.play" %% "play-ws-standalone-xml" % "1.0.1"
libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-core" % "5.4.10"
libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-http-streams" % "5.4.10"
libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-http" % "5.4.10"
libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s-tcp" % "5.4.10"

