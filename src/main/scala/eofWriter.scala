/* Writes EOFs of climate datasets to netCDF file */

package org.apache.spark.mllib.climate

import ucar.nc2.{NetcdfFileWriter, Dimension, Variable, Attribute};
import ucar.ma2.{DataType, ArrayChar, ArrayFloat}
import java.util.ArrayList
import breeze.linalg.{DenseMatrix, DenseVector, convert}
import scala.util.control.Breaks._

object writeEOFs {

    type BDV = DenseVector[Float]
    type BDM = DenseMatrix[Float]

    /* TODO: write out the weights that were applied, so can check after the fact */
    /* Reorder the column dates so the temporal vectors are in time order */
    /* Refactor this so it's easy to use with different datasets (e.g. temperature, 
       multiple atmospheric variables) without hard coding the logic */

    def writeEOFs(fname: String, lats: BDV, lons: BDV, 
                  depths: BDV, dates: Array[String], 
                  mapToLocations: Array[Long], preprocessMethod: String, 
                  mean: BDV, U: BDM, S: BDV, 
                  V: BDM) = {

        /*** Write the metadata ***/

        var writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, fname, null);

        val latDim = writer.addDimension(null, "lat", lats.length)
        val lonDim = writer.addDimension(null, "lon", lons.length)
        val depthDim = writer.addDimension(null, "depth", depths.length)
        val eofsDim = writer.addDimension(null, "eofs", S.length)
        val datesDim = writer.addDimension(null, "dates", V.rows)
        val datelengthDim = writer.addDimension(null, "lengthofdates", dates(0).length())

        var latVar = writer.addVariable(null, "lat", DataType.FLOAT, "lat")
        var lonVar = writer.addVariable(null, "lon", DataType.FLOAT, "lon")
        var depthVar = writer.addVariable(null, "depth", DataType.FLOAT, "depth")
        var coldates = writer.addVariable(null, "coldates", DataType.CHAR, "dates lengthofdates")

        var temporalEOFs = writer.addVariable(null, "temporalEOFs", DataType.FLOAT, "eofs dates")
        var singVals = writer.addVariable(null, "singvals", DataType.FLOAT, "eofs")

        val fillValue : Float = 9999

        val statAttr = new Attribute("type_of_statistical_processing", preprocessMethod)
        val depthAttr = new Attribute("level_type", "Depth below sea level (m)")
        val gridAttr = new Attribute("grid_type", "Latitude/longitude")
        val fillAttr = new Attribute("_FillValue", fillValue)

        var meanEOF = writer.addVariable(null, "meanTemps", DataType.FLOAT, "depth lat lon");
        meanEOF.addAttribute(statAttr)
        meanEOF.addAttribute(depthAttr)
        meanEOF.addAttribute(gridAttr)
        meanEOF.addAttribute(fillAttr)

        for(eofIdx <- 0 until S.length) {
            val curEOF = writer.addVariable(null, "EOF" + eofIdx.toString, DataType.FLOAT, "depth lat lon");
            curEOF.addAttribute(statAttr)
            curEOF.addAttribute(depthAttr)
            curEOF.addAttribute(gridAttr)
            curEOF.addAttribute(fillAttr)
        }

        writer.create()
        writer.close()

        /*** Write the actual values ***/

        writer = NetcdfFileWriter.openExisting(fname)

        write1DFloatArray(writer, "lat", convert(lats, Float))
        write1DFloatArray(writer, "lon", convert(lons, Float))
        write1DFloatArray(writer, "depth", convert(depths, Float))
        writeStringArray(writer, "coldates", dates)

        write2DFloatArray(writer, "temporalEOFs", convert(V.t, Float))
        write1DFloatArray(writer, "singvals", convert(S, Float))
        writeEOF(writer, "meanTemps", mapToLocations, convert(mean, Float), fillValue)
        for (idx <- 0 until S.length) 
            writeEOF(writer, "EOF" + idx.toString, mapToLocations, convert(U(::, idx), Float), fillValue)
       
        writer.close()
    }

    def writeEOF(writer: NetcdfFileWriter, varname: String, writeIndices: Array[Long], eofValues: BDV, fillValue: Float) {
        val eofVar = writer.findVariable(varname)
        val shape = eofVar.getShape()
        val eofVals = new ArrayFloat.D3(shape(0), shape(1), shape(2))

        var curIndex = 0
        var curWriteIndexOffset = 0
        for(depth <- 0 until shape(0); lat <- 0 until shape(1); lon <- 0 until shape(2)) {
           if (curWriteIndexOffset == eofValues.size) 
                eofVals.set(depth, lat, lon, fillValue)
           else if (curIndex == writeIndices(curWriteIndexOffset)) {
                eofVals.set(depth, lat, lon, eofValues(curWriteIndexOffset))
                curWriteIndexOffset += 1
           } else
                eofVals.set(depth, lat, lon, fillValue)
           curIndex += 1
        }

        writer.write(eofVar, eofVals)
    }


    def write1DFloatArray(writer: NetcdfFileWriter, varname: String, arrVals : BDV) = {
       val ncVar = writer.findVariable(varname)
       val shape = ncVar.getShape()
       val varVals = new ArrayFloat.D1(shape(0))
       val varIdx = varVals.getIndex()
       for( valIdx <- 0 until shape(0)) {
            varVals.setFloat(varIdx.set(valIdx), arrVals(valIdx))
       }
       writer.write(ncVar, varVals)
    }

    def write2DFloatArray(writer: NetcdfFileWriter, varname: String, arrVals : BDM) = {
        val ncVar = writer.findVariable(varname)
        val shape = ncVar.getShape()
        val varVals = new ArrayFloat.D2(shape(0), shape(1))
        for ( row <- 0 until shape(0) ) {
            for ( col <- 0 until shape(1) ) {
                varVals.set(row, col, arrVals(row, col))
            }
        }
        writer.write(ncVar, varVals)
    }

    def writeStringArray(writer: NetcdfFileWriter, varname: String, stringVals : Array[String]) = {
        val ncVar = writer.findVariable(varname)
        val shape = ncVar.getShape()
        val varVals = new ArrayChar.D2(shape(0), shape(1))
        val varIdx = varVals.getIndex()
        for( valIdx <- 0 until shape(0)) {
            varVals.setString(varIdx.set(valIdx), stringVals(valIdx))
        }
        writer.write(ncVar, varVals)
    }
}
        


