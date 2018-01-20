name := "portal"
version := "1.0"
scalaVersion := "2.11.8"

parallelExecution in Test := false
scalacOptions ++= Seq("-unchecked", "-deprecation")

initialCommands in console := "import edu.drexel.cs.dbgroup.temporalgraph._"

mainClass in (Compile, packageBin) := Some("edu.drexel.cs.dbgroup.temporalgraph.portal.PortalShell")
mainClass in (Compile, run) := Some("edu.drexel.cs.dbgroup.temporalgraph.portal.PortalShell")

test in assembly := {}

libraryDependencies ++= {

    Seq(
        "com.esotericsoftware" % "kryo" % "3.0.3" % "provided",
        "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
        "org.apache.spark" %% "spark-graphx" % "2.0.0" % "provided",
        "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided",
        "org.apache.spark" %% "spark-hive" % "2.0.0" % "provided",
        "org.apache.spark" %% "spark-mllib" % "2.0.0" % "provided",
        "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
        "org.scala-lang.modules" % "scala-jline" % "2.12.1" % "provided",
        "org.scalactic" % "scalactic_2.11" % "3.0.0-M16-SNAP3" % "test",
        "org.scalatest" % "scalatest_2.11" % "3.0.0-M16-SNAP3" % "test",
        "it.unimi.dsi" % "fastutil" % "7.0.11"
    )

}

dependencyOverrides ++= {

    Set(
        "org.apache.hadoop" % "hadoop-client" % "2.6.0"
    )

}


