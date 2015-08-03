/* Computes the EOFs of large climate datasets with no missing data
 */
object computeEOFs {

  case class EOFDecomposition(leftVectors: DenseMatrix, singularValues: DenseVector, rightVectors: DenseMatrix) {
    def U: DenseMatrix = leftVectors
    def S: DenseVector = singularValues
    def V: DenseMatrix = rightVectors
  }

  def fromBreeze(mat: BDM[Double]): DenseMatrix = {
    new DenseMatrix(mat.rows, mat.cols, mat.dta, mat.isTranspose)
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

    val mat= loadCSVClimateData(sc, inpath, numrows, numcols, preprocessMethod)
    mat.cache()

    val climateEOFs = computeEOFs(mat, numeofs)
    writeOut(outdest, climateEOFs)
  }

  def writeOut(outdest: String, eofs: EOFDecomposition) {
    
  }

  def loadCSVClimateData(sc: SparkContext, inpath: String, numrows: Long, numcols: Int, preprocessMethod: String) : IndexedRowMatrix = {
 	val rows = sc.textFile(inpath).split(",").map(x => (x(1).toInt, (x(0).toInt, x(2).toDouble))).groupByKey.map(x => new IndexedRow(x._1, DenseVector(x._2.toSeq.sortBy(_._1).toArray)))
	new IndexedRowMatrix(rows, numrows, numcols)
  }

  def computeEOFs(mat: IndexedRowMatrix, numeofs: Int) : EOFDecomposition = {
    var mymat = mat
    mymat.cache()

    val (u, v) = getLowRankFactorization(mymat, numeofs)
    convertLowRankFactorizationToEOFs(u, v)
  }

  def convertLowRankFactorizationToEOFs(u : DenseMatrix, v : DenseMatrix) : EOFDecomposition = {
    vsvd = svd(v)
    EOFDecomposition(fromBreeze(u.toBreeze * vsvd.U.toBreeze), vsvd.S, fromBreeze(vsvd.Vt.toBreeze.t))
  }

  // returns U V with k columns so that U*V.t is an optimal rank-k approximation to mat
  def getLowRankFactorization(mat: IndexedRowMatrix, rank: Int) : Tuple2[DenseMatrix, DenseMatrix] = {
    val tol = 1e-13
    val maxIter = 30
    val covOperator = ( v: DBV[Double] ) => multiplyCovarianceBy(mat, fromBreeze(v.toDenseMatrix).transpose).toBreeze.asInstanceOf[BDM[Double]].toDenseVector
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
          U += row.vector.asDenseMatrix.t * (row.vector.asDenseMatrix * rhs)
        },
        combOp = (U1, U2) => U1 += U2
      )
    fromBreeze(1.0/mat.numRows * result)
  }
}
