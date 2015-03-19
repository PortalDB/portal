lazy val root = (project in file(".")).
settings(
    name := "GraphX Extensions",
    version := "1.0",
    scalaVersion := "2.10.4",
    scalaSource in Compile <<= baseDirectory(_ / "."),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.2.1"
)
