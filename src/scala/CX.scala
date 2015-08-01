package org.apache.spark.mllib.linalg.distributed
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{Matrices, DenseMatrix, Matrix, DenseVector, Vector, SparseVector}
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.sql.{SQLContext, Row => SQLRow}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Axis, qr, svd, sum, SparseVector => BSV}
import math.{ceil, log}

import spray.json._
import DefaultJsonProtocol._
import java.util.Arrays
import java.io.{DataOutputStream, BufferedOutputStream, FileOutputStream, File}

object CX {
  def fromBreeze(mat: BDM[Double]): DenseMatrix = {
    // FIXME: does not support strided matrices (e.g. views)
    new DenseMatrix(mat.rows, mat.cols, mat.data, mat.isTranspose)
  }

  // Returns `mat.transpose * mat * rhs`
  def multiplyGramianBy(mat: IndexedRowMatrix, rhs: DenseMatrix): DenseMatrix = {
    val rhsBrz = rhs.toBreeze.asInstanceOf[BDM[Double]]
    val result =
      mat.rows.treeAggregate(BDM.zeros[Double](rhs.numCols, mat.numCols.toInt))(
        seqOp = (U: BDM[Double], row: IndexedRow) => {
          val rowBrz = row.vector.toBreeze.asInstanceOf[BSV[Double]]
          val tmp: BDV[Double] = rhsBrz.t * rowBrz
          // performs a rank-1 update:
          //   U += outer(row.vector, tmp)
          for(ipos <- 0 until rowBrz.index.length) {
            val i = rowBrz.index(ipos)
            val ival = rowBrz.data(ipos)
            for(j <- 0 until tmp.length) {
              U(j, i) += ival * tmp(j)
            }
          }
          U
        },
        combOp = (U1, U2) => U1 += U2
      ).t
    fromBreeze(result)
  }

  def transposeMultiply(mat: IndexedRowMatrix, rhs: DenseMatrix): DenseMatrix = {
    require(mat.numRows == rhs.numRows)
    val rhsBrz = rhs.toBreeze.asInstanceOf[BDM[Double]]
    val result =
      mat.rows.treeAggregate(BDM.zeros[Double](mat.numCols.toInt, rhs.numCols))(
        seqOp = (U: BDM[Double], row: IndexedRow) => {
          val rowIdx = row.index.toInt
          val rowBrz = row.vector.toBreeze.asInstanceOf[BSV[Double]]
          // performs a rank-1 update:
          //   U += outer(row.vector, rhs(row.index, ::))
          for(ipos <- 0 until rowBrz.index.length) {
            val i = rowBrz.index(ipos)
            val ival = rowBrz.data(ipos)
            for(j <- 0 until rhs.numCols) {
              U(i, j) += ival * rhsBrz(rowIdx, j)
            }
          }
          U
        },
        combOp = (U1, U2) => U1 += U2
      )
    fromBreeze(result)
  }

  // returns `mat.transpose * randn(m, rank)`
  def gaussianProjection(mat: IndexedRowMatrix, rank: Int): DenseMatrix = {
    val rng = new java.util.Random
    //transposeMultiply(mat, DenseMatrix.randn(mat.numRows.toInt, rank, rng))
    DenseMatrix.randn(mat.numCols.toInt, rank, rng)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("CX")
    conf.set("spark.task.maxFailures", "1")
    val sc = new SparkContext(conf)

    if(args(0) == "test") {
      testMain(sc, args.tail)
    } else if(args(0) == "genmat") {
      genmatMain(sc, args.tail)
    } else {
      appMain(sc, args)
    }
  }

  def genmatMain(sc: SparkContext, args: Array[String]) = {
    if(args.length != 2) {
      Console.err.println("Expected args: inpath outpath")
      System.exit(1)
    }

    val sqlctx = new org.apache.spark.sql.SQLContext(sc)
    import sqlctx.implicits._

    val inpath = args(0)
    val outpath = args(1)
    val rows = sc.textFile(inpath).map(_.split(",")).
        map(x => (x(1).toInt, (x(0).toInt, x(2).toDouble))).
        groupByKey.
        map(x => (x._1, x._2.toSeq.sortBy(_._1)))
    rows.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val rowtabRdd = rows.keys.distinct(128).sortBy(identity)
    rowtabRdd.persist(StorageLevel.MEMORY_AND_DISK)
    rowtabRdd.saveAsTextFile(outpath + "/rowtab.txt")

    val coltabRdd = rows.values.flatMap(_.map(_._1)).distinct(128).sortBy(identity)
    coltabRdd.persist(StorageLevel.MEMORY_AND_DISK)
    coltabRdd.saveAsTextFile(outpath + "/coltab.txt")

    val rowtab: Array[Int] = rowtabRdd.collect
    val coltab: Array[Int] = coltabRdd.collect
    def rowid(i: Int) = Arrays.binarySearch(rowtab, i)
    def colid(i: Int) = Arrays.binarySearch(coltab, i)
    val newRows = rows.map(x => {
        val indices = x._2.map(y => colid(y._1)).toArray
        val values = x._2.map(y => y._2).toArray
        new IndexedRow(rowid(x._1), new SparseVector(coltab.length, indices, values))
    }).toDF
    newRows.saveAsParquetFile(outpath + "/matrix.parquet")
  }

  def appMain(sc: SparkContext, args: Array[String]) = {
    if(args.length < 8) {
      Console.err.println("Expected args: [csv|idxrow|df] inpath nrows ncols outpath rank slack niters [nparts]")
      System.exit(1)
    }

    val sqlctx = new org.apache.spark.sql.SQLContext(sc)
    import sqlctx.implicits._

    val matkind = args(0)
    val inpath = args(1)
    val shape = (args(2).toInt, args(3).toInt)
    val outpath = args(4)

    // rank of approximation
    val rank = args(5).toInt

    // extra slack to improve the approximation
    val slack = args(6).toInt

    // number of power iterations to perform
    val numIters = args(7).toInt

    val k = rank + slack
    val mat0: IndexedRowMatrix =
      if(matkind == "csv") {
        val nonzeros = sc.textFile(inpath).map(_.split(",")).
        map(x => new MatrixEntry(x(1).toLong, x(0).toLong, x(2).toDouble))
        val coomat = new CoordinateMatrix(nonzeros, shape._1, shape._2)
        val mat = coomat.toIndexedRowMatrix()
        //mat.rows.saveAsObjectFile(s"hdfs:///$name.rowmat")
        mat
      } else if(matkind == "idxrow") {
        val rows = sc.objectFile[IndexedRow](inpath)
        new IndexedRowMatrix(rows, shape._1, shape._2)
      } else if(matkind == "df") {
        val numRows = if(shape._1 != 0) shape._1 else sc.textFile(inpath + "/rowtab.txt").count.toInt
        val numCols = if(shape._2 != 0) shape._2 else sc.textFile(inpath + "/coltab.txt").count.toInt
        val rows =
          sqlctx.parquetFile(inpath + "/matrix.parquet").rdd.map {
            case SQLRow(index: Long, vector: Vector) =>
              new IndexedRow(index, vector)
          }
        new IndexedRowMatrix(rows, numRows, numCols)
      } else {
        throw new RuntimeException(s"unrecognized matkind: $matkind")
      }

    // coalesce partitions if needed
    val mat: IndexedRowMatrix =
      if(args.length >= 9) {
        val nparts = args(8).toInt
        new IndexedRowMatrix(mat0.rows.coalesce(nparts), mat0.numRows, mat0.numCols.toInt)
      } else {
        mat0
      }

    mat.rows.cache()

    // force materialization of the RDD so we can separate I/O from compute
    mat.rows.count()

    /* perform randomized SVD of A' */
    var Y = gaussianProjection(mat, k).toBreeze.asInstanceOf[BDM[Double]]
    for(i <- 0 until numIters) {
      Y = multiplyGramianBy(mat, fromBreeze(Y)).toBreeze.asInstanceOf[BDM[Double]]
      Y = qr.reduced.justQ(Y)
    }
    println("performing QR")
    val Q = qr.reduced.justQ(Y)
    println("done performing QR")
    assert(Q.cols == k)
    val B = mat.multiply(fromBreeze(Q)).toBreeze.asInstanceOf[BDM[Double]].t
    println("performing SVD")
    val Bsvd = svd.reduced(B)
    println("done performing SVD")
    // Since we computed the randomized SVD of A', unswap U and V here
    // to get back to svd(A) = U S V'
    val V = (Q * Bsvd.U).apply(::, 0 until rank)
    val S = Bsvd.S(0 until rank)
    val U = Bsvd.Vt(0 until rank, ::).t

    /* compute leverage scores */
    val rowlev = sum(U :^ 2.0, Axis._1)
    val rowp = rowlev / rank.toDouble
    val collev = sum(V :^ 2.0, Axis._1)
    val colp = collev / rank.toDouble
    assert(rowp.length == mat.numRows)
    assert(colp.length == mat.numCols)

    /* write output */
    val outf = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(outpath))))
    dump(outf, S)
    dump(outf, rowp)
    dump(outf, colp)
    outf.close()
  }

  def dump(outf: DataOutputStream, v: BDV[Double]) = {
    outf.writeInt(v.length)
    for(i <- 0 until v.length) {
      outf.writeDouble(v(i))
    }
  }
  def loadMatrixA(sc: SparkContext, fn: String) = {
    val input = scala.io.Source.fromFile(fn).getLines()
    require(input.next() == "%%MatrixMarket matrix coordinate real general")
    val dims = input.next().split(' ').map(_.toInt)
    val seen = BDM.zeros[Int](dims(0), dims(1))
    val entries = input.map(line => {
      val toks = line.split(" ")
      val i = toks(0).toInt - 1
      val j = toks(1).toInt - 1
      val v = toks(2).toDouble
      require(toks.length == 3)
      new MatrixEntry(i, j, v)
    }).toSeq
    require(entries.length == dims(2))
    new CoordinateMatrix(sc.parallelize(entries, 1), dims(0), dims(1)).toIndexedRowMatrix
  }

  def loadMatrixB(fn: String) = {
    val input = scala.io.Source.fromFile(fn).getLines()
    require(input.next() == "%%MatrixMarket matrix coordinate real general")
    val dims = input.next().split(' ').map(_.toInt)
    val seen = BDM.zeros[Int](dims(0), dims(1))
    val result = BDM.zeros[Double](dims(0), dims(1))
    var count = 0
    input.foreach(line => {
      val toks = line.split(" ")
      require(toks.length == 3)
      val i = toks(0).toInt - 1
      val j = toks(1).toInt - 1
      val v = toks(2).toDouble
      require(i >= 0 && i < dims(0))
      require(j >= 0 && j < dims(1))
      require(seen(i, j) == 0)
      seen(i, j) = 1
      result(i, j) = v
      if(v != 0) count += 1
    })
    //assert(count == dims(2))
    fromBreeze(result)
  }

  def writeMatrix(mat: DenseMatrix, fn: String) = {
    val writer = new java.io.FileWriter(new java.io.File(fn))
    writer.write("%%MatrixMarket matrix coordinate real general\n")
    writer.write(s"${mat.numRows} ${mat.numCols} ${mat.numRows*mat.numCols}\n")
    for(i <- 0 until mat.numRows) {
      for(j <- 0 until mat.numCols) {
        writer.write(f"${i+1} ${j+1} ${mat(i, j)}%f\n")
      }
    }
    writer.close
  }

  def testMain(sc: SparkContext, args: Array[String]) = {
    val A = loadMatrixA(sc, args(0))
    val B = fromBreeze(BDM.ones[Double](1024, 32))
    val X = multiplyGramianBy(A, B)
    writeMatrix(X, "result.mtx")
  }
}
