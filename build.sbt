import sbtassembly.AssemblyPlugin.autoImport.assemblyMergeStrategy

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

name := "boston_crimes_analysis"

version := "0.7.6"

scalaVersion := "2.11.12"


Compile/mainClass := Some("com.kcmamu.boston.BostonCrimes")

lazy val commonSettings = Seq(
  version := "0.1.6-withdistfiller",
  organization := "com.kcmamu",
  scalaVersion := "2.11.12",
  test in assembly := {}
)

lazy val boston_crimes_analysis = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF","services", xs @ _*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF",xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first

}





//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
libraryDependencies += "org.datasyslab" % "geospark" % "1.3.1"
libraryDependencies += "org.datasyslab" % "geospark-sql_2.3" % "1.3.1"
libraryDependencies += "mrpowers" % "spark-daria" % "0.35.2-s_2.11"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true)

// test suite settings

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests



testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
fork in Test := true

// JAR file settings

// don't include Scala in the JAR file
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Add the JAR file naming conventions described here: https://github.com/MrPowers/spark-style-guide#jar-files
// You can add the JAR file naming conventions by running the shell script
