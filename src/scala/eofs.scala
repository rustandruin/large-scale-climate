/* Computes the EOFs of large climate datasets with missing data
 * If X = 
 *
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
    val inpath = args(0)
    val numrows = args(1).toInt
    val numcols = args(2).toInt
    val imputation = args(3)
    val preprocessMethod = args(4)
    val outdest = args(5)
    val numeofs = args(6).toInt
    val slack = args(7).toInt
    val niters = args(8).toInt
    val nparts = if (args.length >= 10) { args(9).toInt } else {0}

    val matAndMask = loadClimateData(sc, inpath, shape, nparts)
    val mask = matAndMask._2
    mask.cache()
    val mat = preprocess(matAndMask._1, mask, preprocessMethod)

    val climateEOFs = computeDINEOF(mat, mask, numeofs)
    // determine quality of EOFs and write out
    writeOut(outdest, climateEOFs)
  }

  def preprocess(mat: IndexedRowMatrix, mask: IndexedRowMatrix, method: String) : IndexedRowMatrix = {

  }

  def loadClimateData(sc: SparkContext, inpath: String, shape: Tuple2[Int, Int], nparts: Int) : Tuple2[IndexedRowMatrix, IndexedRowMatrix] = {
  
  }

  // does a fixed number of iterations of the matrix completion process
  // return (U, S, V) so that U*S*V't is a low-rank approximation
  def computeDINEOF(mat: IndexedRowMatrix, mask: IndexedRowMatrix, numeofs: Int, numiters: Int) {
    var mymat = mat

    // initially fill missing data with zeros
    for( iter <- 0 until numiters) { 
      mymat.cache() // re-cache for the iterations needed for low-rank approximation
      val (u, v) = getLowRankFactorization(mymat, numeofs)
      mymat = interpolateMissing(mymat, mask, u, v)
    }

    convertLowRankFactorizationToEOFs(u, v)
  }

  def convertLowRankFactorizationToEOFs(u : DenseMatrix, v : DenseMatrix) : EOFDecomposition = {
    vsvd = svd(v)
    EOFDecomposition(fromBreeze(u.toBreeze * vsvd.U.toBreeze), vsvd.S, fromBreeze(vsvd.Vt.toBreeze.t))
  }

  // assumes mat and mask have corresponding partitions
  def interpolateMissing(mat: IndexedRowMatrix, mask: IndexedRowMatrix, u: DenseMatrix, v: DenseMatrix) : IndexedRowMatrix = {

    def replaceMissingEntries(row: IndexedRow, mask: IndexedRow) : IndexedRow = {
      assert(row.index == mask.index)
      val boolMask = mask.vector.toBreeze.asInstanceOf[BSV[Double]]
      val newrow = row.vector
      for( colidx <- 0 until boolMask.index.length) {
        val colpos = boolMask.index(colidx)
        newrow(colpos) = u(row.index.toInt, ::) * v(::, colpos)
      }
      IndexedRow(row.index, newrow)
    }

    new IndexedRowMatrix(mat.rows.zip(mask.rows).map((x) => replaceMissingEntries(x._1, x._2)), mat.numRows, mat.numCols.toInt)
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
