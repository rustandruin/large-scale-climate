/* 
TODO: should give option of collecting the covariance chunks back on the executor (preferable, because then can do mat-vec products purely locally),
 or distributing them (for use when there is not enough memory on the executor to store them); in the latter case, should be able to put multiple chunks
 on a single executor, depending on memory availability, so that minimize communication overhead. 
 TODO: take advantage of the fact that the covar matrix is PSD, so only need the upper triangular portion
 TODO: allow arbitray rectangular chunkings of the upper triangular portion of covar matrix, so can adapt to memory availability: e.g. the first chunk of rows can be divided across two executors as two large chunks, while the remaining chunks of rows can be stored in one executor (b/c of the upper triangularity, storage size needed decreases)
 */

package org.apache.spark.mllib.climate

import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer
import scala.math.{sqrt, ceil, log, rint, max}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, norm}

import org.slf4j.LoggerFactory

object chunkedCovariancePCA { 
  // for block partitioned covariance matrix (first tuple is row partition [start, end), second is col partition [start, end), DenseMatrix is the actual chunk)
  type BlockIndices = Tuple2[Int, Int]
  type DenseMatrixChunk = Tuple3[BlockIndices, BlockIndices, BDM[Double]]

  def partitionIndices( vectorLength: Int, numPartitions: Int) : Array[BlockIndices] = {
      val smallChunkSize = vectorLength/numPartitions
      val bigChunkSize = vectorLength/numPartitions + 1
      val numBigChunks = vectorLength - (vectorLength/numPartitions)*numPartitions
      val numSmallChunks = numPartitions - numBigChunks

      require(vectorLength/numPartitions > 0, s"The requested number of partitions $numPartitions is larger than the number of columns in the covariance matrix $vectorLength")

      require(bigChunkSize*bigChunkSize < Integer.MAX_VALUE,
          s"The maximum allowed number of elements in a partition of the covariance matrix is ${Integer.MAX_VALUE}, currently the maximum number of elements is $bigChunkSize")

      val chunkStarts = NumericRange(0, smallChunkSize*numSmallChunks, smallChunkSize).toArray ++ NumericRange(smallChunkSize*numSmallChunks, vectorLength, bigChunkSize)
      val chunkEnds = NumericRange(smallChunkSize, smallChunkSize*(numSmallChunks + 1), smallChunkSize).toArray ++ NumericRange(smallChunkSize*numSmallChunks + bigChunkSize, vectorLength + 1, bigChunkSize)

      chunkStarts zip chunkEnds
  }

  // note this will make numPartitions^2 passes over the data to collect the covariance matrix
  def collectChunkedCovarianceMatrix(sc: SparkContext, mat: IndexedRowMatrix, numPartitions: Int) : RDD[DenseMatrixChunk] = {
      val chunkIndices = partitionIndices(mat.numCols.toInt, numPartitions)
      val numObs = mat.numRows.toDouble
      val broadcastChunkIndices = sc.broadcast(chunkIndices)
      val logger = LoggerFactory.getLogger(getClass)

      def computeCovarianceChunks(rowIdx : Int, colIdx : Int ) : Function[Iterator[IndexedRow], Iterator[BDM[Double]]] = 
        (rows: Iterator[IndexedRow]) =>{
          val lazyrows = rows.toSeq 
          val (rowColStart, rowColEnd) = broadcastChunkIndices.value(rowIdx)
          val (colColStart, colColEnd) = broadcastChunkIndices.value(colIdx)
          val rowColChunk = BDM.zeros[Double](lazyrows.length, rowColEnd - rowColStart)
          val colColChunk = BDM.zeros[Double](lazyrows.length, colColEnd - colColStart)
          for (rowidx <- 0 until lazyrows.length) {
            val currow = BDV(lazyrows(rowidx).vector.toArray)
            rowColChunk(rowidx, ::) := currow(rowColStart until rowColEnd).t
            colColChunk(rowidx, ::) := currow(colColStart until colColEnd).t
          } 
          Iterator(1/numObs*rowColChunk.t*colColChunk)
        }

      var chunkedCovarianceRDD : RDD[DenseMatrixChunk] = sc.emptyRDD[DenseMatrixChunk]
      var chunkArray = new ArrayBuffer[DenseMatrixChunk]

      for( rowIdx <- 0 until numPartitions; 
           colIdx <- 0 to rowIdx) {
          val zeroVal = BDM.zeros[Double](chunkIndices(rowIdx)._2 - chunkIndices(rowIdx)._1,
                                          chunkIndices(colIdx)._2 - chunkIndices(colIdx)._1)
          logger.info(s"Computing chunk ${colIdx + rowIdx*numPartitions + 1} of ${(numPartitions*(numPartitions+1))/2} of the covariance matrix")
          logger.info(s"This chunk is size ${zeroVal.rows}-by-${zeroVal.cols}")
          // val treeDepth = ceil(log(max(mat.rows.partitions.size, 2))/log(2)).toInt // use a binary reduction tree
          val treeDepth = 2 // hard-coded for now (seems a deeper tree is more expensive due to communication since we're passing around large matrices ?)
          // treeDepth really should be computed based on memory: e.g. 2698-by-2698 DP chunks are about 54MB, so assuming we have 1GB on each executor that will be collecting,
          // about 20 will fit, so we can use a tree of depth ceil(log(numPartitions)/log(20)) to reduce
          logger.info(s"Using a reduction tree of depth $treeDepth")
          val newChunkSummands = mat.rows.mapPartitions(computeCovarianceChunks(rowIdx, colIdx))
	  logger.info(s"done computing the ${newChunkSummands.count} summands for this chunk")
	  val newChunk = newChunkSummands.treeReduce(_+_, depth=treeDepth)

          logger.info("Done computing chunk")
          chunkArray += ((chunkIndices(rowIdx), chunkIndices(colIdx), newChunk))

//          chunkedCovarianceRDD = chunkedCovarianceRDD ++
//              sc.parallelize(Array((chunkIndices(rowIdx), chunkIndices(colIdx), newChunk))) // hopefully Spark is smart enough not to put too many chunks on one executor
      }

      chunkedCovarianceRDD = sc.parallelize(chunkArray)
      chunkedCovarianceRDD
  }

  def multiplyChunkedCovarianceBy(covar: RDD[DenseMatrixChunk], rhs: BDM[Double]) = {
    // covar contains just the upper triangular blocks of the covariance matrix 
    // chunk._1 contains the starting and ending row indices
    // chunk._2 contains the starting and ending column indices
    // chunk._3 contains the actual chunk of the covariance matrix
    val blockchunks = covar.flatMap( chunk => 
          if (chunk._1 == chunk._2)
            Iterator( (chunk._1, 0.5 * chunk._3 * rhs(chunk._2._1 until chunk._2._2, ::)),
              (chunk._2, 0.5 * chunk._3 * rhs(chunk._1._1 until chunk._1._2, ::)) )
          else
            Iterator( (chunk._1, chunk._3 * rhs(chunk._2._1 until chunk._2._2, ::)),
              (chunk._2, chunk._3 * rhs(chunk._1._1 until chunk._1._2, ::)) )
        ).collect

    val uniqueRowChunkIndices = blockchunks.map(pair => pair._1).distinct
    val result = BDM.zeros[Double](rhs.rows, rhs.cols)
    for(rowChunkIdx <- 0 until uniqueRowChunkIndices.length) {
      val (start, end) = uniqueRowChunkIndices(rowChunkIdx)
      val rowChunk = blockchunks.filter(pair => pair._1 == (start, end)).map(pair => pair._2).reduceLeft(_ + _)
      result(start until end, ::) := rowChunk
    }
    result
  }

  def main( args: Array[String]) = {
    val conf = new SparkConf().setAppName("testChunkedCovariancePCA")
    val sc = new SparkContext(conf)
    sys.addShutdownHook( { sc.stop() } )

    val numRows = 51
    val numCols = 10000

    val origMat = BDM.rand(numRows, numCols)
    val rowRDD = sc.parallelize( (0 until numRows).map( idx => new IndexedRow(idx, new DenseVector(origMat(idx, ::).t.toArray))), 10)
    val mat = new IndexedRowMatrix(rowRDD)

    val covarRDD = collectChunkedCovarianceMatrix(sc, mat, 5)

    val testrhs = BDM.rand(numCols, 30)
    val chunkedProd = multiplyChunkedCovarianceBy(covarRDD, testrhs)
    val origProd = 1/numRows.toDouble*origMat.t*(origMat * testrhs)
    val diff = norm(chunkedProd.toDenseVector - origProd.toDenseVector)
    val tol = 1e-7
    assert( norm(diff) < tol, "Difference is not smaller than the tolerance level")
    println(s"Norm of difference: ${norm(chunkedProd.toDenseVector - origProd.toDenseVector)}")
  }

}
