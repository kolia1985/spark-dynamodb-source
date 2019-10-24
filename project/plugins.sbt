//addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"


resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

//addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")

addSbtPlugin("com.github.alonsodomin" % "sbt-spark" % "0.6.0")

//addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

//addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")

//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")