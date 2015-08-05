package org.apache.spark.climate
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.sql.{SQLContext, Row => SQLRow}
import java.util.Arrays

object CSVToParquet {
  def main(args: Array[String]) = {
    val conf= new SparkConf().setAppName("CSV to Parquet convertor").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    convertCSVToScala(sc, args)
  }

  def convertCSVToScala(sc: SparkContext, args: Array[String]) = {
    if(args.length != 3) {
      Console.err.println("Expected args: inpath maskpath outpath") 
      System.exit(1)
    }

    val sqlctx = new org.apache.spark.sql.SQLContext(sc)
    import sqlctx.implicits._

    val valsinpath = args(0)
    val maskinpath = args(1) 
    val outpath = args(2)

    // figure out which locations have missing observations so we can drop them
    val droprows: Array[Int] = sc.textFile(maskinpath).map(_.split(",")).map(x => x(1).toInt).distinct().collect
    val valsrows = sc.textFile(valsinpath).map(_.split(",")).map(x => (x(1).toInt, (x(0).toInt, x(2).toDouble))).filter(x => !droprows.contains(x._1)).groupByKey.map(x => (x._1, x._2.toSeq.sortBy(_._1)))
    valsrows.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val valsrowtabRdd = valsrows.keys.distinct(100).sortBy(identity)
    valsrowtabRdd.persist(StorageLevel.MEMORY_AND_DISK)
    valsrowtabRdd.saveAsTextFile(args(1) + "/valsrowtab.txt")

    val coltabRdd = valsrows.values.flatMap(_.map(_._1)).distinct(100).sortBy(identity)
    coltabRdd.persist(StorageLevel.MEMORY_AND_DISK)
    coltabRdd.saveAsTextFile(args(1) + "/coltab.txt")

    val valsrowtab: Array[Int] = valsrowtabRdd.collect
    val coltab: Array[Int] = coltabRdd.collect
    def valsrowid(i: Int) = Arrays.binarySearch(valsrowtab, i)
    def colid(i: Int) = Arrays.binarySearch(coltab, i)

    val newValsRows = valsrows.map(x => {
      val values = x._2.map(y => y._2).toArray
      new IndexedRow(valsrowid(x._1), new DenseVector(values))
    }).toDF
    newValsRows.saveAsParquetFile(outpath + "/vals.parquet")
  }
}
