/*
 * Copyright 2017 Iaroslav Zeigerman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import sbt._
import Keys._
import scoverage.ScoverageKeys._

object TwinkleBuild extends Build {

  val SparkVersion = "2.1.0"
  val ScalaTestVersion = "2.2.6"

  val CommonSettings = Seq(
    organization := "com.github.izeigerman",
    scalaVersion := "2.11.8",
    version := "0.1-SNAPSHOT",

    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-feature",
      "-language:postfixOps",
      "-language:implicitConversions",
      "-language:higherKinds"),

    parallelExecution in Test := false,

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % SparkVersion,
      "org.apache.spark" %% "spark-sql" % SparkVersion,
      "org.apache.spark" %% "spark-mllib" % SparkVersion,
      "org.scalatest" %% "scalatest" % ScalaTestVersion % "test->*"
    )
  )

  val NoPublishSettings = Seq(
    publishArtifact := false,
    publish := {},
    coverageEnabled := false
  )

  lazy val root = Project(id = "root", base = file("."))
    .settings(NoPublishSettings: _*)
    .aggregate(twinkle)

  lazy val twinkle = Project(id = "twinkle", base = file("twinkle"))
    .settings(CommonSettings: _*)
}

