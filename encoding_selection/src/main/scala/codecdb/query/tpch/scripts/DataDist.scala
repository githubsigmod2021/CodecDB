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
object DataDist extends App {

  val schema = TPCHSchema.lineitemSchema
  //  val inputFolder = "/home/harper/TPCH/"
  val inputFolder = args(0)
  val suffix = ".parquet"
  val file = new File("%s%s%s".format(inputFolder, schema.getName, suffix)).toURI

  val colIndex = args(1).toInt

  val recorder = new RowTempTable(schema);

  val thresholds = Array(0.05, 0.1, 0.25, 0.5, 0.75, 0.9)

  var sum = 0.0
  var count = 0L

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
  var varsum = 0.0
  // Compute Variance
  ParquetReaderHelper.read(file, new EncReaderProcessor() {

    override def processRowGroup(version: ParsedVersion, meta: BlockMetaData, rowGroup: PageReadStore): Unit = {
      val col = schema.getColumns()(colIndex)
      val reader = new ColumnReaderImpl(col, rowGroup.getPageReader(col),
        recorder.getConverter(colIndex).asPrimitiveConverter(), version)

      for (counter <- 0L until meta.getRowCount) {

        col.getType match {
          case PrimitiveTypeName.INT32 => {
            varsum += Math.pow(reader.getInteger - mean, 2)
          }
          case PrimitiveTypeName.DOUBLE => {
            varsum += Math.pow(reader.getDouble - mean, 2)
          }
          case _ => throw new IllegalArgumentException()
        }
      }
    }
  })

  varsum = varsum / count
  println(max)
  println(min)
  println(mean)
  println(varsum)
}
