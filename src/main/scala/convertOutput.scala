package org.apache.spark.mllib.linalg.distributed
import breeze.linalg.{DenseMatrix, DenseVector}
import java.io.{DataInputStream, FileInputStream, FileWriter, File}

object ConvertDump { 

  type DM = DenseMatrix[Double]
  type DV = DenseVector[Double]

  def loadVector( inf: DataInputStream) : DV = {
    val len = inf.readInt()
    val v = DenseVector.zeros[Double](len)
    for (i <- 0 until len) {
      v(i) = inf.readDouble()
    }
    v
  }
  
  def loadMatrix( inf: DataInputStream) : DM = {
    val (r,c) = Tuple2(inf.readInt(), inf.readInt())
    val m = DenseMatrix.zeros[Double](r,c)
    for (i <- 0 until r; j <- 0 until c) {
      m(i,j) = inf.readDouble()
    }
    m 
  }

  def loadDump(infname: String) : Tuple3[DM, DM, DV] = {

    val inf = new DataInputStream( new FileInputStream(infname))

    val eofsU = loadMatrix(inf)
    val eofsV = loadMatrix(inf)
    val mean = loadVector(inf)

    inf.close()
    (eofsU, eofsV, mean)
  }

  def writeMatrix(mat: DM, fn: String) = {
    val writer = new FileWriter(new File(fn))
    writer.write("%%MatrixMarket matrix coordinate real general\n")
    writer.write(s"${mat.rows} ${mat.cols} ${mat.rows*mat.cols}\n")
    for(i <- 0 until mat.rows) {
      for(j <- 0 until mat.cols) {
        writer.write(f"${i+1} ${j+1} ${mat(i, j)}%f\n")
      }
    }
    writer.close
  }

  def main(args: Array[String]) {
    val (eofsU, eofsV, mean) = loadDump(args(0))
    writeMatrix(eofsU, s"${args(1)}/colEOFs")
    writeMatrix(eofsV, s"${args(1)}/rowEOFs")
    writeMatrix(mean.asDenseMatrix, s"${args(1)}/rowMeans")
  }
}
