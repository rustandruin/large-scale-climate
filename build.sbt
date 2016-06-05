version := "0.0.1"
scalaVersion := "2.11.8"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1" % "provided"
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2"

val awskey = System.getenv("AWS_ACCESS_KEY_ID")
val awssecretkey = System.getenv("AWS_SECRET_ACCESS_KEY")

lazy val submit = taskKey[Unit]("Compute CSFR-0 3D EOFs")
submit <<= (assembly in Compile) map {
  (jarFile: File) => s"src/computeEOFs.sh ${jarFile}" !
} 
