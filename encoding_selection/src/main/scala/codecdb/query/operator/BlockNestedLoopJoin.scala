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
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */

package codecdb.query.operator

import java.net.URI

import codecdb.parquet.{EncReaderProcessor, ParquetReaderHelper}
import codecdb.query.{ColumnTempTable, PipePrimitiveConverter, TempTable}
import codecdb.query.util.{DataUtils, SchemaUtils}
import edu.uchicago.cs.encsel.parquet.EncReaderProcessor
import edu.uchicago.cs.encsel.query.util.SchemaUtils
import edu.uchicago.cs.encsel.query.ColumnTempTable
import org.apache.parquet.VersionParser
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class BlockNestedLoopJoin(val numBlock: Int) extends Join {

  def join(left: URI, leftSchema: MessageType, right: URI, rightSchema: MessageType, joinKey: (Int, Int),
           leftProject: Array[Int], rightProject: Array[Int]): TempTable = {

    val leftKeySchema = SchemaUtils.project(leftSchema, Array(joinKey._1))
    val rightKeySchema = SchemaUtils.project(rightSchema, Array(joinKey._2))
    val leftProjectSchema = SchemaUtils.project(leftSchema, leftProject)
    val rightProjectSchema = SchemaUtils.project(rightSchema, rightProject)

    val leftKeys = new Array[ColumnTempTable](numBlock)
    val rightKeys = new Array[ColumnTempTable](numBlock)
    val leftBlocks = new Array[ColumnTempTable](numBlock)
    val rightBlocks = new Array[ColumnTempTable](numBlock)

    val outputSchema = SchemaUtils.join(leftSchema, rightSchema, leftProject, rightProject)
    val output = new ColumnTempTable(outputSchema)

    for (i <- 0 until numBlock) {
      leftKeys(i) = new ColumnTempTable(leftKeySchema)
      rightKeys(i) = new ColumnTempTable(rightKeySchema)
      leftBlocks(i) = new ColumnTempTable(leftProjectSchema)
      rightBlocks(i) = new ColumnTempTable(rightProjectSchema)
    }
    // Fill in left group
    ParquetReaderHelper.read(left, new EncReaderProcessor() {

      override def processRowGroup(version: VersionParser.ParsedVersion, meta: BlockMetaData, rowGroup: PageReadStore) = {
        val colConverters = (0 until leftProjectSchema.getColumns.size())
          .map(i => new PipePrimitiveConverter(leftProjectSchema.getType(i).asPrimitiveType()))
        val colReaders = leftProjectSchema.getColumns.zip(colConverters).map(col =>
          new ColumnReaderImpl(col._1, rowGroup.getPageReader(col._1), col._2, version))

        val keyCol = leftSchema.getColumns()(joinKey._1)
        val keyConverter = new PipePrimitiveConverter(leftSchema.getType(joinKey._1).asPrimitiveType())
        val keyReader = new ColumnReaderImpl(keyCol, rowGroup.getPageReader(keyCol), keyConverter, version)

        val placements = new ArrayBuffer[Int]()
        for (i <- 0L until rowGroup.getRowCount) {
          val keyValue = DataUtils.readValue(keyReader)
          val keyIndex = (keyValue.hashCode() % numBlock).toInt
          placements += keyIndex
          keyConverter.setNext(leftKeys(keyIndex).getConverter(0).asPrimitiveConverter())

          keyReader.writeCurrentValueToConverter()
          keyReader.consume()
        }

        colReaders.zipWithIndex.foreach(rp => {
          val reader = rp._1
          val index = rp._2
          for (i <- 0L until rowGroup.getRowCount) {
            val placement = placements(i.toInt)
            colConverters(index).setNext(leftBlocks(placement).getConverter(index).asPrimitiveConverter())
            reader.writeCurrentValueToConverter()
            reader.consume()
          }
        })
      }
    })
    // Fill in right group
    ParquetReaderHelper.read(right, new EncReaderProcessor() {

      override def processRowGroup(version: VersionParser.ParsedVersion, meta: BlockMetaData, rowGroup: PageReadStore) = {
        val colConverters = (0 until rightProjectSchema.getColumns.size())
          .map(i => new PipePrimitiveConverter(rightProjectSchema.getType(i).asPrimitiveType()))
        val colReaders = rightProjectSchema.getColumns.zip(colConverters).map(col =>
          new ColumnReaderImpl(col._1, rowGroup.getPageReader(col._1), col._2, version))

        val keyCol = rightSchema.getColumns()(joinKey._2)
        val keyConverter = new PipePrimitiveConverter(rightSchema.getType(joinKey._2).asPrimitiveType())
        val keyReader = new ColumnReaderImpl(keyCol, rowGroup.getPageReader(keyCol), keyConverter, version)

        val placements = new ArrayBuffer[Int]()
        for (i <- 0L until rowGroup.getRowCount) {
          val keyValue = DataUtils.readValue(keyReader)
          val keyIndex = (keyValue.hashCode() % numBlock).toInt
          placements += keyIndex
          keyConverter.setNext(rightKeys(keyIndex).getConverter(0).asPrimitiveConverter())
          keyReader.writeCurrentValueToConverter()
          keyReader.consume()
        }

        colReaders.zipWithIndex.foreach(rp => {
          val reader = rp._1
          val index = rp._2
          for (i <- 0L until rowGroup.getRowCount) {
            val placement = placements(i.toInt)
            colConverters(index).setNext(rightBlocks(placement).getConverter(index).asPrimitiveConverter())
            reader.writeCurrentValueToConverter()
            reader.consume()
          }
        })
      }
    })
    // Join each group

    val pairs = new ArrayBuffer[(Int, Int)]()

    for (i <- 0 until numBlock) {
      val leftKey = leftKeys(i)
      val rightKey = rightKeys(i)

      val leftBlock = leftBlocks(i)
      val rightBlock = rightBlocks(i)

      var leftCounter = 0
      var rightCounter = 0

      leftKey.getColumns()(0).getData.foreach(left => {
        rightKey.getColumns()(0).getData.foreach(right => {
          // TODO only support EQJoin right now
          if (left.equals(right)) {
            pairs += ((leftCounter, rightCounter))
          }
          rightCounter += 1
        })
        leftCounter += 1
      })
      // Export data from left blocks
      leftBlock.getColumns.zipWithIndex.foreach(entry => {
        val column = entry._1
        val index = entry._2
        pairs.foreach(pair => {
          DataUtils.writeValue(output.getConverter(index).asPrimitiveConverter(), column.getData()(pair._1))
        })
      })

      // Export data from right blocks to row buffer
      rightBlock.getColumns.zipWithIndex.foreach(entry => {
        val column = entry._1
        val index = entry._2
        pairs.foreach(pair => {
          DataUtils.writeValue(output.getConverter(leftBlock.getColumns.size + index).asPrimitiveConverter(),
            column.getData()(pair._2))
        })
      })
    }
    return output
  }
}
