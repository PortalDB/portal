lazy val root = (project in file(".")).
settings(
    name := "Temporal Graph Project",
    version := "1.0",
    scalaVersion := "2.10.4",
    scalaSource in Compile <<= baseDirectory(_ / "."),
    initialCommands in console := "import edu.drexel.cs.dbgroup.temporalgraph._",
    mainClass in (Compile, packageBin) := Some("edu.drexel.cs.dbgroup.temporalgraph.portal.PortalShell"),
    mainClass in (Compile, run) := Some("edu.drexel.cs.dbgroup.temporalgraph.portal.PortalShell"),
    assemblyJarName in assembly := "tgraph-assembly-1.0.jar",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.5.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
    libraryDependencies += "org.scala-lang.modules" % "scala-jline" % "2.12.1",
    dependencyOverrides += "org.apache.hadoop" % "hadoop-client" % "2.6.0"
)
