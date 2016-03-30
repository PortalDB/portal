lazy val root = (project in file(".")).
settings(
    name := "GraphX Extensions",
    version := "1.0",
    scalaVersion := "2.10.5",
    scalaSource in Compile <<= baseDirectory(_ / "."),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.0" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
    libraryDependencies += "org.scala-lang.modules" % "scala-jline" % "2.12.1",
    dependencyOverrides += "org.apache.hadoop" % "hadoop-client" % "2.6.0"
)
