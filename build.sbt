version := "0.0.1"
scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1" % "provided"
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2"
libraryDependencies += "io.spray" %% "spray-json" % "1.3.2"

val awskey = System.getenv("AWS_ACCESS_KEY_ID")
val awssecretkey = System.getenv("AWS_SECRET_ACCESS_KEY")

lazy val runConversion = taskKey[Unit]("Convert GRIB Data to Parquet")
runConversion <<= (assembly in Compile) map {
  (jarFile: File) => s"src/runparallelconversion.sh ${awskey} ${awssecretkey}" !
}

lazy val runTest = taskKey[Unit]("Submit climate test job")
runTest <<= (assembly in Compile) map {
  (jarFile: File) => s"spark-submit --driver-memory 4G --class org.apache.mllib.linalg.distributed.computeEOF ${jarFile} test" !
}

lazy val submit = taskKey[Unit]("Compute CSFR-0 3D EOFs")
submit <<= (assembly in Compile) map {
  (jarFile: File) => s"src/computeEOFs.sh ${jarFile}" !
} 
