lazy val root = (project in file(".")).
settings(
    name := "Temporal Graph Project",
    version := "1.0",
    scalaVersion := "2.10.5",
    scalaSource in Compile <<= baseDirectory(_ / "."),
    parallelExecution in Test := false,
    initialCommands in console := "import edu.drexel.cs.dbgroup.temporalgraph._",
    mainClass in (Compile, packageBin) := Some("edu.drexel.cs.dbgroup.temporalgraph.portal.PortalShell"),
    mainClass in (Compile, run) := Some("edu.drexel.cs.dbgroup.temporalgraph.portal.PortalShell"),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.0" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.0" % "provided",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
    libraryDependencies += "it.unimi.dsi" % "fastutil" % "7.0.11",
    libraryDependencies += "org.scala-lang.modules" % "scala-jline" % "2.12.1",
    libraryDependencies += "org.scalactic" % "scalactic_2.10" % "3.0.0-M16-SNAP3",
    libraryDependencies += "org.scalatest" % "scalatest_2.10" % "3.0.0-M16-SNAP3",
    libraryDependencies += "com.esotericsoftware" % "kryo" % "3.0.3",
    dependencyOverrides += "org.apache.hadoop" % "hadoop-client" % "2.6.0"
)
