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

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0"
compileOrder := CompileOrder.JavaThenScala
unmanagedClasspath in Compile += baseDirectory.value / "src" / "main" / "morphadorner-src"
unmanagedClasspath in Runtime += baseDirectory.value / "src" / "main" / "morphadorner-src"
