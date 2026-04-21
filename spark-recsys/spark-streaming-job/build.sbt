ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-recsys-job",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.5.1" % Provided,
      "org.apache.spark" %% "spark-mllib" % "3.5.1" % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
      "redis.clients" % "jedis" % "5.1.5"
    ),
    assembly / assemblyJarName := "spark-recsys-job.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) =>
        xs.map(_.toLowerCase) match {
          case "services" :: _ => MergeStrategy.concat
          case _ => MergeStrategy.discard
        }
      case _ => MergeStrategy.first
    }
  )
