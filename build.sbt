
enablePlugins(SparkPlugin)

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "nmelnik",
      scalaVersion := "2.11.1"
    )),
    name := "spark-dynamodb-source",
    version := "0.0.2",
    sparkVersion := "2.4.0",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    sparkComponents ++= Seq("core", "sql", "catalyst"),
    parallelExecution in Test := false,
    fork := true,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    libraryDependencies ++= Seq(
      "com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.5.0" % "provided",
      "org.scalatest" %% "scalatest" % "3.0.1" % "provided",
      "com.amazonaws" %  "aws-java-sdk-sts"  % "1.11.253" % "provided",
      "com.amazonaws" %  "aws-java-sdk-core" % "1.11.313",
      "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.253" % "provided",
      "com.github.traviscrawford" % "spark-dynamodb" % "0.0.12",
      "com.amazonaws" % "amazon-kinesis-client" % "1.8.10" % "provided",
      "com.amazonaws" % "DynamoDBLocal" % "1.11.86" % "test",
      "com.almworks.sqlite4java" % "libsqlite4java-osx" % "1.0.392" % "test",
      "com.almworks.sqlite4java" % "sqlite4java" % "1.0.392" % "test",
      "com.almworks.sqlite4java" % "sqlite4java-win32-x86" % "1.0.392" % "test",
      "com.almworks.sqlite4java" % "sqlite4java-win32-x64" % "1.0.392" % "test",
      "com.almworks.sqlite4java" % "libsqlite4java-linux-i386" % "1.0.392" % "test",
      "com.almworks.sqlite4java" % "libsqlite4java-linux-amd64" % "1.0.392" % "test",
      "org.eclipse.jetty" % "jetty-server" % "9.3.11.v20160721" % "test",
      "org.eclipse.jetty" % "jetty-client" % "9.3.11.v20160721" % "test",
      "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests"
    ),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },
    resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
      "dynamodblocal" at "https://s3-us-west-2.amazonaws.com/dynamodb-local/release",
      Resolver.sonatypeRepo("public")
    ),
    // publish settings
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    }
    //    , dbcUsername := sys.env("username")
    //    , dbcPassword := ""

  )

lazy val copyJars = taskKey[Unit]("copyJars")
copyJars := {
  import java.nio.file.Files
  import java.io.File
  // For Local Dynamo DB to work, we need to copy SQLLite native libs from
  // our test dependencies into a directory that Java can find ("lib" in this case)
  // Then in our Java/Scala program, we need to set System.setProperty("sqlite4java.library.path", "lib");
  // before attempting to instantiate a DynamoDBEmbedded instance
  val artifactTypes = Set("dylib", "so", "dll")
  val files = Classpaths.managedJars(Test, artifactTypes, update.value).files
  Files.createDirectories(new File(baseDirectory.value, "native-libs").toPath)
  files.foreach { f =>
    val fileToCopy = new File("native-libs", f.name)
    if (!fileToCopy.exists()) {
      Files.copy(f.toPath, fileToCopy.toPath)
    }
  }
}

(compile in Compile) := (compile in Compile).dependsOn(copyJars).value