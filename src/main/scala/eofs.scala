/* Computes the EOFs of large climate datasets with no missing data
 *
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{DenseVector, DenseMatrix}
import org.apache.spark.mllib.linalg.EigenValueDecomposition
import org.apache.spark.mllib.linalg.distributed._
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, svd}

 */

package org.apache.spark.mllib.climate
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{Matrices, DenseMatrix, Matrix, DenseVector, Vector, SparseVector, Vectors}
import org.apache.spark.mllib.linalg.EigenValueDecomposition
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.sql.{SQLContext, Row => SQLRow}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Axis, qr, svd, sum, SparseVector => BSV}
import breeze.linalg.{norm, diag, accumulate}
import breeze.numerics.{sqrt => BrzSqrt}
import math.{ceil, log}
import scala.collection.mutable.ArrayBuffer

import java.util.Arrays
import java.io.{DataOutputStream, BufferedOutputStream, FileOutputStream, File}

object computeEOFs {

  case class EOFDecomposition(leftVectors: DenseMatrix, singularValues: DenseVector, rightVectors: DenseMatrix) {
    def U: DenseMatrix = leftVectors
    def S: DenseVector = singularValues
    def V: DenseMatrix = rightVectors
  }

  def fromBreeze(mat: BDM[Double]): DenseMatrix = {
    new DenseMatrix(mat.rows, mat.cols, mat.data, mat.isTranspose)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("ClimateEOFs")
    conf.set("spark.task.maxFailures", "1")
    val sc = new SparkContext(conf)
    appMain(sc, args)
  }

  def appMain(sc: SparkContext, args: Array[String]) = {
    val matformat = args(0)
    val inpath = args(1)
    val numrows = args(2).toLong
    val numcols = args(3).toInt
    val preprocessMethod = args(4)
    val numeofs = args(5).toInt
    val outdest = args(6)

    val (mat, mean)= loadCSVClimateData(sc, matformat, inpath, numrows, numcols, preprocessMethod)

    val (u, v) = getLowRankFactorization(mat, numeofs)
    val climateEOFs = convertLowRankFactorizationToEOFs(u, v)
    writeOut(outdest, climateEOFs, mean)
  }

  def writeOut(outdest: String, eofs: EOFDecomposition, mean: BDV[Double]) {
    val outf = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(outdest))))
    dumpMat(outf, eofs.U.toBreeze.asInstanceOf[BDM[Double]])
    dumpMat(outf, eofs.V.toBreeze.asInstanceOf[BDM[Double]])
    dumpV(outf, eofs.S.toBreeze.asInstanceOf[BDV[Double]])
    dumpV(outf, mean)
  }

  def dumpV(outf: DataOutputStream, v: BDV[Double]) = {
    outf.writeInt(v.length) 
    for(i <- 0 until v.length) {
      outf.writeDouble(v(i))
    }
  }

  def dumpMat(outf: DataOutputStream, mat: BDM[Double]) = {
    outf.writeInt(mat.rows)
    outf.writeInt(mat.cols)
    for(i <- 0 until mat.rows) {
      for(j <- 0 until mat.cols) {
        outf.writeDouble(mat(i,j))
      }
    }
  }

  // For now, assume input is always csv and that rows are all observed, and that only mean centering is desired
  def loadCSVClimateData(sc: SparkContext, matformat: String, inpath: String, numrows: Long, 
    numcols: Int, preprocessMethod: String) : Tuple2[IndexedRowMatrix, BDV[Double]] = {
    val rows = sc.textFile(inpath).map(x => x.split(",")).
      map(x => (x(1).toInt, (x(0).toInt, x(2).toDouble))).
      groupByKey.map(x => new IndexedRow(x._1, new 
        DenseVector(x._2.toSeq.sortBy(_._1).map(_._2).toArray)))
    rows.persist(StorageLevel.MEMORY_AND_DISK)
    val mat = new IndexedRowMatrix(rows, numrows, numcols)
    val mean = getRowMean(mat)
    val centeredmat = subtractMean(mat, mean)
    centeredmat.rows.persist(StorageLevel.MEMORY_AND_DISK)
    mat.rows.unpersist()
    centeredmat.rows.count()
    (centeredmat, mean)
  }

  def getRowMean(mat: IndexedRowMatrix) : BDV[Double] = {
    1.0/mat.numRows * mat.rows.treeAggregate(BDV.zeros[Double](mat.numCols.toInt))(
      seqOp = (avg: BDV[Double], row: IndexedRow) => {
        val rowBrz = row.vector.toBreeze.asInstanceOf[BDV[Double]]
        avg += rowBrz
      },
      combOp = (avg1, avg2) => avg1 += avg2
      )
  }

  def subtractMean(mat: IndexedRowMatrix, mean: BDV[Double]) : IndexedRowMatrix = {
    val centeredrows = mat.rows.map(x => new IndexedRow(x.index, 
      new DenseVector((x.vector.toBreeze.asInstanceOf[BDV[Double]] - mean).toArray)))
    new IndexedRowMatrix(centeredrows, mat.numRows, mat.numCols.toInt)
  }

  def convertLowRankFactorizationToEOFs(u : DenseMatrix, v : DenseMatrix) : EOFDecomposition = {
    val vBrz = v.toBreeze.asInstanceOf[BDM[Double]]
    val vsvd = svd(vBrz)
    EOFDecomposition(fromBreeze(u.toBreeze.asInstanceOf[BDM[Double]] * vsvd.U), new DenseVector(vsvd.S.data), fromBreeze(vsvd.Vt.t))
  }

  // returns U V with k columns so that U*V.t is an optimal rank-k approximation to mat
  def getLowRankFactorization(mat: IndexedRowMatrix, rank: Int) : Tuple2[DenseMatrix, DenseMatrix] = {
    val tol = 1e-13
    val maxIter = 30
    val covOperator = ( v: BDV[Double] ) => multiplyCovarianceBy(mat, fromBreeze(v.toDenseMatrix).transpose).toBreeze.asInstanceOf[BDM[Double]].toDenseVector
    val (lambda, u) = EigenValueDecomposition.symmetricEigs(covOperator, mat.numCols.toInt, rank, tol, maxIter)
    (fromBreeze(u), leftMultiplyBy(mat, fromBreeze(u.t)))
  }

  // computes BA where B is a local matrix and A is distributed: let b_i denote the
  // ith col of B and a_i denote the ith row of A, then BA = sum(b_i a_i)
  def leftMultiplyBy(mat: IndexedRowMatrix, lhs: DenseMatrix) : DenseMatrix = {
   val lhsFactor = mat.rows.context.broadcast(lhs.toBreeze.asInstanceOf[BDM[Double]])

   val result =
     mat.rows.treeAggregate(BDM.zeros[Double](lhs.numRows.toInt, mat.numCols.toInt))(
       seqOp = (U: BDM[Double], row: IndexedRow) => {
         val rowBrz = row.vector.toBreeze.asInstanceOf[BDV[Double]]
         U += (lhsFactor.value)(::, row.index.toInt) * rowBrz.t
       },
       combOp = (U1, U2) => U1 += U2, depth = 2
     )
   fromBreeze(result)
  }

  // Returns `1/n * mat.transpose * mat * rhs`
  def multiplyCovarianceBy(mat: IndexedRowMatrix, rhs: DenseMatrix): DenseMatrix = {
    val rhsBrz = rhs.toBreeze.asInstanceOf[BDM[Double]]
    val result = 
      mat.rows.treeAggregate(BDM.zeros[Double](rhs.numCols, mat.numCols.toInt))(
        seqOp = (U: BDM[Double], row: IndexedRow) => {
          val rowBrz = row.vector.toBreeze.asInstanceOf[BDV[Double]]
          U += row.vector.toBreeze.asInstanceOf[BDV[Double]].asDenseMatrix.t * (row.vector.toBreeze.asInstanceOf[BDV[Double]].asDenseMatrix * rhs.toBreeze.asInstanceOf[BDM[Double]])
        },
        combOp = (U1, U2) => U1 += U2
      )
    fromBreeze(1.0/mat.numRows * result)
  }
}
