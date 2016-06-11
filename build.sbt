version := "0.0.1"
scalaVersion := "2.10.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1" % "provided"
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2" 
libraryDependencies += "org.msgpack" %% "msgpack-scala" % "0.6.11"

lazy val submit = taskKey[Unit]("Compute CSFR-0 3D EOFs")
submit <<= (assembly in Compile) map {
  (jarFile: File) => s"src/computeEOFs.sh ${jarFile}" !
} 

lazy val testEOFs = taskKey[Unit]("Compute CSFRO 3D EOFs")
testEOFs <<= (assembly in Compile) map {
  (jarFile : File) => s"src/testEOFs.sh ${jarFile}" !
}
