resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/", Resolver.mavenLocal)

name := "Streaming Analysis"

version := "0.1-SNAPSHOT"

organization := "net.teralytics"

scalaVersion in ThisBuild := "2.11.7"

lazy val flinkVersion = "1.1.3"

def flinkDependency(version: String)(library: String): ModuleID =
  "org.apache.flink" %% s"flink-$library" % version

lazy val flink = flinkDependency(flinkVersion) _

lazy val flinkDependencies = Seq(
  flink("scala"),
  flink("streaming-scala"),
  flink("connector-kafka-0.8"))

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

mainClass in assembly := Some("net.teralytics.flink.demo.StreamingAnalysis")

// make run command include the provided dependencies
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
