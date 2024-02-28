scalaVersion := "2.12.11"
version := "0.1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.17.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.0",
  "org.apache.spark" %% "spark-core" % "3.3.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided",
  "com.google.code.gson" % "gson" % "2.8.9"
)

// Assembly settings
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x if x.endsWith("FastHashMap.class") => MergeStrategy.first
  case x if x.endsWith("package-info.class") => MergeStrategy.first
  // Add other specific cases here as needed
  case "module-info.class" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}


lazy val utils = (project in file("."))
  .settings(
    assembly / assemblyJarName := "sparketl.jar"
  )
