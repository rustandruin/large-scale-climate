package org.apache.spark.climate
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.sql.{SQLContext, Row => SQLRow}
import java.util.Arrays

object CSVtoParquet {
  def main(args: Array[String]) = {
    val conf= new SparkConf().setAppName("CSV to Parquet convertor")
    val sc = new SparkContext(conf)

    convertCSVToScala(sc, args)
  }

  def convertCSVToScala(sc: SparkContext, args: Array[String]) = {
    if(args.length != 2) {
      Console.err.println("Expected args: inpath outpath") 
      System.exit(1)
    }

    val sqlctx = new org.apache.spark.sql.SQLContext(sc)
    import sqlctx.implicits._

    val valsinpath = args(0) + "/vals"
    val maskinpath = args(0) + "/mask"
    val valsoutpath = args(1) + "/vals"
    val maskoutpath = args(1) + "/mask"
    val valsrows = sc.textFile(valsinpath).map(_.split(",")).map(x => (x(1).toInt, (x(0).toInt, x(2).toDouble))).groupByKey.map(x => (x._1, x._2.toSeq.sortBy(_._1)))
    valsrows.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val maskrows = sc.textFile(maskinpath).map(_.split(",")).map(x => (x(1).toInt, (x(0).toInt, x(2).toDouble))).groupByKey.map(x => (x._1, x._2.toSeq.sortBy(_._1)))
    maskrows.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val rowtabRdd = rows.keys.distinct(100).sortBy(identity)
    rowtabRdd.persist(StorageLevel.MEMORY_AND_DISK)
    rowtabRdd.saveAsTextFile(args(1) + "/rowtab.txt")

    val coltabRdd = rows.values.flatMap(_.map(_._1)).distinct(100).sortBy(identity)
    coltabRdd.persist(StorageLevel.MEMORY_AND_DISK)
    coltabRdd.saveAsTextFile(args(1) + "/coltab.txt")

    val valsrowtab: Array[Int] = rowtabRdd.collect
    val coltab: Array[Int] = coltabRdd.collect
    def rowid(i: Int) = Arrays.binarySearch(rowtab, i)
    def colid(i: Int) = Arrays.binarySearch(coltab, i)

    val newValsRows = valsrows.map(x => {
      val values = x._2.map(y => y._2).toArray
      new IndexedRow(rowid(x._1), new DenseVector(values))
    }).toDF
    newValsRows.saveAsParquetFile(valsoutpath + "/matrix.parquet")

    val newMaskRows = maskrows.map(x => {
      val indices = x._2.map(y => colid(y._1)).toArray
      val values = x._2.map(y => y._2).toArray
      new IndexedRow(rowid(x._1), new SparseVector(coltab.length, indices, values))
    }).toDF
    newMaskRows.saveAsParquetFile(maskoutpath + "/matrix.parquet")
  }
}
