package org.apache.spark.mllib.climate

import scala.collection.immutable.NumericRange
import scala.math.{sqrt, ceil, log, rint, max}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, norm}

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

  def collectChunkedCovarianceMatrix(sc: SparkContext, mat: IndexedRowMatrix, numPartitions: Int) : RDD[DenseMatrixChunk] = {
      val chunkIndices = partitionIndices(mat.numCols.toInt, numPartitions)
      val broadcastChunkIndices = sc.broadcast(chunkIndices)

      def computeCovarianceChunks(rowIdx : Int, colIdx : Int ) : Function[Iterator[IndexedRow], Iterator[BDM[Double]]] = 
        (rows: Iterator[IndexedRow]) =>{
          val lazyrows = rows.toSeq // why the HELL does Scala consume Iterators when you ask for their length?; think this is lazier than using an array/list?
          val (rowColStart, rowColEnd) = broadcastChunkIndices.value(rowIdx)
          val (colColStart, colColEnd) = broadcastChunkIndices.value(colIdx)
          val rowColChunk = BDM.zeros[Double](lazyrows.length, rowColEnd - rowColStart)
          val colColChunk = BDM.zeros[Double](lazyrows.length, colColEnd - colColStart)
          for (rowidx <- 0 until lazyrows.length) {
            val currow = BDV(lazyrows(rowidx).vector.toArray)
            rowColChunk(rowidx, ::) := currow(rowColStart until rowColEnd).t
            colColChunk(rowidx, ::) := currow(colColStart until colColEnd).t
          } 
          Iterator(rowColChunk.t*colColChunk)
        }

      var chunkedCovarianceRDD : RDD[DenseMatrixChunk] = sc.emptyRDD[DenseMatrixChunk]

      for( rowIdx <- 0 until numPartitions; colIdx <- 0 until numPartitions ) {
          val zeroVal = BDM.zeros[Double](chunkIndices(rowIdx)._2 - chunkIndices(rowIdx)._1,
                                          chunkIndices(colIdx)._2 - chunkIndices(colIdx)._1)
          val treeDepth = ceil(log(max(mat.rows.partitions.size, 2))/log(2)).toInt
          val newChunk = mat.rows.mapPartitions(computeCovarianceChunks(rowIdx, colIdx)).
                         treeAggregate(zeroVal)( _ + _, _ + _, depth=treeDepth)
          chunkedCovarianceRDD = chunkedCovarianceRDD ++
              sc.parallelize(Array((chunkIndices(rowIdx), chunkIndices(colIdx), newChunk))) // hopefully Spark is smart enough not to put too many chunks on one executor
      }

      chunkedCovarianceRDD
  }

  def multiplyChunkedCovarianceBy(covar: RDD[DenseMatrixChunk], rhs: BDM[Double]) = {
 
    // chunk._1 contains the starting and ending row indices
    // chunk._2 contains the starting and ending column indices
    // chunk._3 contains the actual chunk of the covariance matrix
    val blockchunks = covar.map( chunk => (chunk._1, chunk._3 * rhs(chunk._2._1 until chunk._2._2, ::)) ).collect
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
    val origProd = origMat.t*(origMat * testrhs)
    val diff = norm(chunkedProd.toDenseVector - origProd.toDenseVector)
    val tol = 1e-7
    assert( norm(diff) < tol, "Difference is not smaller than the tolerance level")
    println(s"Norm of difference: ${norm(chunkedProd.toDenseVector - origProd.toDenseVector)}")
  }

}
