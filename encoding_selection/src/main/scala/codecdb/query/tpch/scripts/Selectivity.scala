/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License,
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 *
 */

package codecdb.query.tpch.scripts

import java.io.File

import codecdb.parquet.{EncReaderProcessor, ParquetReaderHelper}
import codecdb.query.RowTempTable
import codecdb.query.tpch.TPCHSchema
import edu.uchicago.cs.encsel.parquet.EncReaderProcessor
import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.Footer
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import scala.collection.JavaConversions._

/**
  * Look for a value with selectivity 5%, 10%, 25%, 50%, 75% and 90%
  */
object Selectivity extends App {

  val schema = TPCHSchema.lineitemSchema
  //  val inputFolder = "/home/harper/TPCH/"
  val inputFolder = args(0)
  val suffix = ".parquet"
  val file = new File("%s%s%s".format(inputFolder, schema.getName, suffix)).toURI

  val colIndex = args(1).toInt

  val recorder = new RowTempTable(schema);

  val minimal = 0L

  val selectivities = Array(0.05, 0.1, 0.25, 0.5, 0.75, 0.9)
  var selCounts: Array[Long] = null
  var count = 0L
  var selCount = 0L


  val step = 50
  var sum = 0.0

  var max = Double.MinValue
  var min = Double.MaxValue
  // Compute Sum and Mean
  ParquetReaderHelper.read(file, new EncReaderProcessor() {
    override def processFooter(footer: Footer): Unit = {
      super.processFooter(footer)
      count = footer.getParquetMetadata.getBlocks.map(_.getRowCount).sum
    }

    override def processRowGroup(version: ParsedVersion, meta: BlockMetaData, rowGroup: PageReadStore): Unit = {
      val col = schema.getColumns()(colIndex)
      val reader = new ColumnReaderImpl(col, rowGroup.getPageReader(col),
        recorder.getConverter(colIndex).asPrimitiveConverter(), version)

      for (counter <- 0L until meta.getRowCount) {

        col.getType match {
          case PrimitiveTypeName.INT32 => {
            val current = reader.getInteger
            sum += current
            max = Math.max(max, current)
            min = Math.min(min, current)
          }
          case PrimitiveTypeName.DOUBLE => {
            val current = reader.getDouble
            sum += current
            max = Math.max(max, current)
            min = Math.min(min, current)
          }
          case _ => throw new IllegalArgumentException()
        }

      }
    }
  })
  val mean = sum / count

  val serve = (max - min).toDouble / count

  var thresholds = Array.fill[Double](selectivities.length)(max)
  var thresCounter = Array.fill[Long](selectivities.length)(0)

  ParquetReaderHelper.read(file, new EncReaderProcessor() {
    override def processFooter(footer: Footer): Unit = {
      super.processFooter(footer)
      count = footer.getParquetMetadata.getBlocks.map(_.getRowCount).sum
      selCounts = selectivities.map(i => (i * count).toLong)
    }

    override def processRowGroup(version: ParsedVersion, meta: BlockMetaData, rowGroup: PageReadStore): Unit = {
      val col = schema.getColumns()(colIndex)
      val reader = new ColumnReaderImpl(col, rowGroup.getPageReader(col),
        recorder.getConverter(colIndex).asPrimitiveConverter(), version)

      for (counter <- 0L until meta.getRowCount) {

        val current: Double = col.getType match {
          case PrimitiveTypeName.INT32 => {
            reader.getInteger
          }
          case PrimitiveTypeName.DOUBLE => {
            reader.getDouble
          }
          case _ => throw new IllegalArgumentException()
        }

        for (i <- 0 until thresholds.length) {
          var thres = thresholds(i)
          var thresCount = thresCounter(i) + 1
          val thresUpper = selCounts(i)
          if (current < thres) {
            if (thresCount < thresUpper) {
              thres = Math.max(thres, current)
            } else {
              // Already used up, now need to shrink the size
              val subtract = step
              thres -= subtract
              thresCount -= Math.ceil(subtract / serve).toLong
            }
          }
          thresholds(i) = thres
          thresCounter(i) = thresCount
        }
      }
    }
  })

  println(step)
  println(serve)
  println(selCounts.mkString(","))
  println(thresCounter.mkString(","))
  println(thresholds.mkString(","))
}
