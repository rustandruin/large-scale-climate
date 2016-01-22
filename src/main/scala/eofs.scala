/* Computes the EOFs of large climate datasets with no missing data
 *
 */

package org.apache.spark.mllib.climate
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{Matrices, DenseMatrix, Matrix, DenseVector, Vector, SparseVector, Vectors}
//import org.apache.spark.mllib.linalg.EigenValueDecomposition
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.sql.{SQLContext, Row => SQLRow}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Axis, qr, svd, sum, SparseVector => BSV}
import breeze.linalg.{norm, diag, accumulate, rank => BrzRank}
import breeze.linalg.{min, argmin}
import breeze.stats.meanAndVariance
import breeze.numerics.{sqrt => BrzSqrt}
import math.{ceil, log}
import scala.collection.mutable.ArrayBuffer

import java.util.Arrays
import java.io.FileOutputStream
import java.util.zip.GZIPOutputStream
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.hadoop.io.compress.DefaultCodec

import org.msgpack.MessagePack;

object computeEOFs {

  def report(message: String, verbose: Boolean = true) = {
    val now = Calendar.getInstance().getTime()
    val formatter = new SimpleDateFormat("H:m:s")

    if(verbose) {
      println("STATUS REPORT (" + formatter.format(now) + "): " + message)
    }
  }

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
    conf.set("spark.task.maxFailures", "1").set("spark.shuffle.blockTransferService", "nio")
    val sc = new SparkContext(conf)
    sys.addShutdownHook( { sc.stop() } )
    appMain(sc, args)
  }

  def appMain(sc: SparkContext, args: Array[String]) = {
    val inpath = args(0)
    val numrows = args(1).toInt
    val numcols = args(2).toLong
    val preprocessMethod = args(3)
    val numeofs = args(4).toInt
    val outdest = args(5)
    
    val info = loadParquetClimateData(sc, inpath, numrows, numcols, preprocessMethod)
    val mat = info.productElement(0).asInstanceOf[IndexedRowMatrix]

    val (u, v) = getLowRankFactorization(mat, numeofs)
    //val (u, v) = getApproxLowRankFactorization(mat, numeofs, 2, 4)
    val climateEOFs = convertLowRankFactorizationToEOFs(u, v)
    writeOut(outdest, climateEOFs, info)

    report(s"U - ${climateEOFs.U.numRows}-by-${climateEOFs.U.numCols}")
    report(s"S - ${climateEOFs.S.size}")
    report(s"V - ${climateEOFs.V.numRows}-by-${climateEOFs.V.numCols}")
    val errorvariance = calcSSE(mat, climateEOFs.U.toBreeze.asInstanceOf[BDM[Double]], 
      diag(BDV(climateEOFs.S.toArray)) * climateEOFs.V.toBreeze.asInstanceOf[BDM[Double]].t)
    val approxvariance = climateEOFs.S.toArray.map(math.pow(_,2)).sum
    val datavariance = mat.rows.map(x => x.vector.toArray.map(math.pow(_,2)).sum).sum
    report(s"Variance of low-rank approximation: $approxvariance")
    report(s"Error variance: $errorvariance")
    report(s"Data variance: $datavariance")
    report(s"$numeofs EOFs explain ${approxvariance/datavariance} of the variance with ${errorvariance/datavariance} relative error")

  }

  // returns the processed matrix as well as a Product containing the information relevant to that processing:
  //
  def loadParquetClimateData(sc: SparkContext, inpath: String, numrows: Int, numcols: Long, preprocessMethod: String) : Product = {

    val sqlctx = new org.apache.spark.sql.SQLContext(sc)
    import sqlctx.implicits._

    val rows = {
      sqlctx.parquetFile(inpath + "/finalmat.parquet").rdd.map {
        case SQLRow(index: Long, vector: Vector) =>
          new IndexedRow(index, vector)
      }
    }//.coalesce(2880)
    //rows.persist(StorageLevel.MEMORY_ONLY_SER)
    val tempmat = new IndexedRowMatrix(rows, numcols, numrows)

    if ("centerOverAllObservations" == preprocessMethod) {
      val mean = getRowMeans(tempmat)
      val centeredmat = subtractMean(tempmat, mean)
      centeredmat.rows.persist(StorageLevel.MEMORY_ONLY_SER)
      centeredmat.rows.count()
      //rows.unpersist()

      (centeredmat, mean)
    }else if ("standardizeEach" == preprocessMethod) {
      val (mean, stdv) = getRowMeansAndStd(tempmat)
      val standardizedmat = standardize(tempmat, mean, stdv)
      standardizedmat.rows.persist(StorageLevel.MEMORY_ONLY_SER)
      standardizedmat.rows.count()
      //rows.unpersist()

      (standardizedmat, mean, stdv)
    }else if ("cosLat+centerOverAllObservations" == preprocessMethod) {
      val mean = getRowMeans(tempmat)
      val centeredmat = subtractMean(tempmat, mean)

      val latitudeweights = BDV.zeros[Double](tempmat.numCols.toInt)
      sc.textFile(inpath + "/latitudeweights").map( line => line.split(",") ).collect.map( pair => latitudeweights(pair(0).toInt) = pair(1).toDouble )

      val reweightedmat = areaWeight(centeredmat, latitudeweights)
      reweightedmat.rows.persist(StorageLevel.MEMORY_ONLY_SER)
      reweightedmat.rows.count()
      //rows.unpersist()

      (reweightedmat, areaWeight(mean, latitudeweights))
    } else {
      None
    }
  }

  def areaWeight(mat: IndexedRowMatrix, weights: BDV[Double]) : IndexedRowMatrix = {
    val reweightedrows = mat.rows.map(x => new IndexedRow(x.index, new DenseVector((x.vector.toBreeze.asInstanceOf[BDV[Double]] :* weights).toArray)) )
    new IndexedRowMatrix(reweightedrows, mat.numRows, mat.numCols.toInt)
  }

  def areaWeight(vec: BDV[Double], weights: BDV[Double]) : BDV[Double] = {
    vec :* weights
  }

  def calcSSE(mat: IndexedRowMatrix, lhsTall: BDM[Double], rhsFat: BDM[Double]) : Double = {
      val sse = mat.rows.treeAggregate(BDV.zeros[Double](1))(
        seqOp = (partial: BDV[Double], row: IndexedRow) => {
          val reconstructed = (lhsTall(row.index.toInt, ::) * rhsFat).t
          partial(0) += math.pow(norm(row.vector.toBreeze.asInstanceOf[BDV[Double]] - reconstructed), 2)
          partial
        },
        combOp = (partial1, partial2) => partial1 += partial2,
        depth = 5
      )
      sse(0)
  }

  // a la https://gist.github.com/thiagozs/6699612
  def writeOut(outdest: String, eofs: EOFDecomposition, info: Product) {
    val outf = new GZIPOutputStream(new FileOutputStream(outdest))
    val msgpack = new MessagePack();
    val packer = msgpack.createPacker(outf)

    packer.write(eofs.U.numCols) // number of EOFs
    packer.write(eofs.U.numRows) // number of observation points
    packer.write(eofs.U.toBreeze.asInstanceOf[BDM[Double]].toDenseVector.toArray)
    packer.write(eofs.V.toBreeze.asInstanceOf[BDM[Double]].toDenseVector.toArray)
    packer.write(eofs.S.toBreeze.asInstanceOf[BDV[Double]].toArray)
    packer.write(info.productElement(1).asInstanceOf[BDV[Double]].toArray)
    if (info.productArity == 3) {
      packer.write(info.productElement(2).asInstanceOf[BDV[Double]].toArray) 
    }
  
    outf.close()
  }

  // returns a column vector of the means of each row
  def getRowMeans(mat: IndexedRowMatrix) : BDV[Double] = {
    val scaling = 1.0/mat.numCols
    val mean = BDV.zeros[Double](mat.numRows.toInt)
    val meanpairs = mat.rows.map( x => (x.index, scaling * x.vector.toArray.sum)).collect()
    //report(s"Computed ${meanpairs.length} row means (presumably nonzero)")
    meanpairs.foreach(x => mean(x._1.toInt) = x._2)
    mean
  }

  // returns two column vectors: one of the means of the row, one of the standard deviation
  // for now, use a two-pass algorithm (one-pass can be unstable)
  def getRowMeansAndStd(mat: IndexedRowMatrix) : Tuple2[BDV[Double], BDV[Double]] = {
    val mean = BDV.zeros[Double](mat.numRows.toInt)
    val std = BDV.zeros[Double](mat.numRows.toInt)
    val meanstdpairs = mat.rows.map( x => (x.index, meanAndVariance(x.vector.toBreeze.asInstanceOf[BDV[Double]])) ).collect()
    //report(s"Computed ${meanstdpairs.length} row means and stdvs (presumably nonzero)")
    meanstdpairs.foreach( x => { mean(x._1.toInt) = x._2.mean; std(x._1.toInt) = x._2.stdDev })
    val stdtol = .0001
    val badpairs = std.toArray.zipWithIndex.filter(x => x._1 < stdtol)
    report(s"Indices of rows with too low standard deviation, and the standard deviations (there are ${badpairs.length} such rows):")
    report(s"${badpairs.map(x => x._2).foldLeft("")( (x, y) => x + " " + y.toString )}")
    badpairs foreach { case(badstd, idx) => report(s"(${idx}, ${badstd})") }
    report(s"Replacing those bad standard deviations with 1s")
    badpairs.foreach(x => std(x._2) = 1)
    (mean, std)
  }

  def subtractMean(mat: IndexedRowMatrix, mean: BDV[Double]) : IndexedRowMatrix = {
    val centeredrows = mat.rows.map(x => new IndexedRow(x.index, new DenseVector(x.vector.toArray.map(v => v - mean(x.index.toInt)))))
    new IndexedRowMatrix(centeredrows, mat.numRows, mat.numCols.toInt)
  }

  def standardize(mat: IndexedRowMatrix, mean: BDV[Double], stdv: BDV[Double]) : IndexedRowMatrix = {
    val standardizedrows = mat.rows.map(x => new IndexedRow(x.index, new DenseVector(x.vector.toArray.map( v => 1./(stdv(x.index.toInt)) * (v - mean(x.index.toInt)) ))))
    new IndexedRowMatrix(standardizedrows, mat.numRows, mat.numCols.toInt)
  }

  def convertLowRankFactorizationToEOFs(u : DenseMatrix, v : DenseMatrix) : EOFDecomposition = {
    val vBrz = v.toBreeze.asInstanceOf[BDM[Double]]
    val vsvd = svd.reduced(vBrz)
    // val diff = vsvd.U*diag(BDV(vsvd.S.data))*fromBreeze(vsvd.Vt.t).toBreeze.asInstanceOf[BDM[Double]].t - vBrz
    // report(s"Sanity check (should be 0): ${diff.data.map(x => x*x).sum}")
    EOFDecomposition(fromBreeze(u.toBreeze.asInstanceOf[BDM[Double]] * vsvd.U), new DenseVector(vsvd.S.data), fromBreeze(vsvd.Vt.t))
  }

  // returns U V with k columns so that U*V.t is an optimal rank-k approximation to mat and U has orthogonal columns
  def getLowRankFactorization(mat: IndexedRowMatrix, rank: Int) : Tuple2[DenseMatrix, DenseMatrix] = {
    val tol = 1e-13
    val maxIter = 30
    val covOperator = ( v: BDV[Double] ) => multiplyCovarianceBy(mat, fromBreeze(v.toDenseMatrix).transpose).toBreeze.asInstanceOf[BDM[Double]].toDenseVector
    val (lambda, u) = EigenValueDecomposition.symmetricEigs(covOperator, mat.numCols.toInt, rank, tol, maxIter)
    report(s"Square Frobenius norm of approximate row basis for data: ${u.data.map(x=>x*x).sum}")
    val Xlowrank = mat.multiply(fromBreeze(u)).toBreeze()
    val qr.QR(q,r) = qr.reduced(Xlowrank)
    report(s"Square Frobenius norms of Q,R: ${q.data.map(x=>x*x).sum}, ${r.data.map(x=>x*x).sum}") 
    (fromBreeze(q), fromBreeze(r*u.t)) 
  }

  // returns U V with k columns so that U*V.t is a low-rank approximate factorization to mat and U has orthogonal columns
  // uses the RSVD algorithm
  def getApproxLowRankFactorization(mat: IndexedRowMatrix, rank: Int, oversample: Int, numIters: Int) : Tuple2[DenseMatrix, DenseMatrix] = {
    val rng = new java.util.Random()
    var Y = DenseMatrix.randn(mat.numCols.toInt, rank + oversample, rng)
    var Ynew = multiplyCovarianceBy(mat, Y)
    Y = fromBreeze(qr.justQ(Ynew.toBreeze.asInstanceOf[BDM[Double]]))
    for( iterIdx <- 0 until numIters-1) {
      Ynew = multiplyCovarianceBy(mat, Y)
      Y = fromBreeze(qr.justQ(Ynew.toBreeze.asInstanceOf[BDM[Double]]))
    }
    val tempsvd = svd.reduced(Y.toBreeze.asInstanceOf[BDM[Double]])
    val Q = tempsvd.U(::, 0 until rank).copy
    (fromBreeze(Q), leftMultiplyBy(mat, fromBreeze(Q).transpose))
  }

  // computes BA where B is a local matrix and A is distributed: let b_i denote the
  // ith col of B and a_i denote the ith row of A, then BA = sum(b_i a_i)
  def leftMultiplyBy(mat: IndexedRowMatrix, lhs: DenseMatrix) : DenseMatrix = {
   report(s"Left multiplying a ${mat.numRows}-by-${mat.numCols} matrix by a ${lhs.numRows}-by-${lhs.numCols} matrix")
   val lhsFactor = mat.rows.context.broadcast(lhs.toBreeze.asInstanceOf[BDM[Double]])

   val result =
     mat.rows.treeAggregate(BDM.zeros[Double](lhs.numRows.toInt, mat.numCols.toInt))(
       seqOp = (U: BDM[Double], row: IndexedRow) => {
         val rowBrz = row.vector.toBreeze.asInstanceOf[BDV[Double]]
         U += (lhsFactor.value)(::, row.index.toInt) * rowBrz.t
       },
       combOp = (U1, U2) => U1 += U2, depth = 3
     )
   fromBreeze(result)
  }

  // Returns `1/n * mat.transpose * mat * rhs`
  def multiplyCovarianceBy(mat: IndexedRowMatrix, rhs: DenseMatrix): DenseMatrix = {
    report(s"Going to multiply the covariance operator of a ${mat.numRows}-by-${mat.numCols} matrix by a ${rhs.numRows}-by-${rhs.numCols} matrix")
    val rhsBrz = rhs.toBreeze.asInstanceOf[BDM[Double]]
    val result = 
      mat.rows.treeAggregate(BDM.zeros[Double](mat.numCols.toInt, rhs.numCols))(
        seqOp = (U: BDM[Double], row: IndexedRow) => {
          val rowBrz = row.vector.toBreeze.asInstanceOf[BDV[Double]]
          U += row.vector.toBreeze.asInstanceOf[BDV[Double]].asDenseMatrix.t * (row.vector.toBreeze.asInstanceOf[BDV[Double]].asDenseMatrix * rhs.toBreeze.asInstanceOf[BDM[Double]])
        },
        combOp = (U1, U2) => U1 += U2
      )
    fromBreeze(1.0/mat.numRows * result)
  }
}
