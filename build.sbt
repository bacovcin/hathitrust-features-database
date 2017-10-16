import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "upenn",
      scalaVersion := "2.11.6",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "hathitrust-features-database",
    libraryDependencies += scalaTest % Test
  )

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.24"
libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.5.4"
libraryDependencies += "net.liftweb" %% "lift-json" % "3.1.0"
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.5"
libraryDependencies += "log4j" % "log4j" % "1.2.14"
libraryDependencies += "com.madhukaraphatak" %% "java-sizeof" % "0.1"

compileOrder := CompileOrder.JavaThenScala
unmanagedClasspath in Compile += baseDirectory.value / "src" / "main" / "morphadorner-src"
unmanagedClasspath in Runtime += baseDirectory.value / "src" / "main" / "morphadorner-src"
