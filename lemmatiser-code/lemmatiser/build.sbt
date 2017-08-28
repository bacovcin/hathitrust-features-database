import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "upenn",
      scalaVersion := "2.12.3",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "Lemmatiser",
    libraryDependencies += scalaTest % Test
  )

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.24"
libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.1"

compileOrder := CompileOrder.JavaThenScala
unmanagedClasspath in Compile += baseDirectory.value / "src" / "main" / "morphadorner-src"
unmanagedClasspath in Runtime += baseDirectory.value / "src" / "main" / "morphadorner-src"
