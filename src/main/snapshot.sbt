lazy val root = (project in file(".")).
settings(
    name := "Snapshot Graph Project",
    version := "1.0",
    scalaVersion := "2.10.4",
    scalaSource in Compile <<= baseDirectory(_ / "."),
    initialCommands in console := "import edu.drexel.cs.dbgroup.graphxt._",
    mainClass in (Compile, packageBin) := Some("edu.drexel.cs.dbgroup.graphxt.Driver"),
    mainClass in (Compile, run) := Some("edu.drexel.cs.dbgroup.graphxt.Driver"),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.2.1",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.0.4",
    dependencyOverrides += "org.apache.hadoop" % "hadoop-client" % "1.0.4"
)
