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
import codecdb.query.{ColumnTempTable, HPredicate, NonePrimitiveConverter, Predicate, PredicatePipe, RowTempTable, TempTable, VPredicate}
import codecdb.query.util.SchemaUtils
import edu.uchicago.cs.encsel.parquet.ReaderProcessor
import edu.uchicago.cs.encsel.query._
import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.Footer
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConversions._

trait Select {
  def select(input: URI, p: Predicate, schema: MessageType,
             projectIndices: Array[Int]): TempTable

  def columnMapping(p: Predicate, schema: MessageType, projectIndices: Array[Int]):
  (Map[Int, Int], Map[Int, Int], Iterable[Int]) = {
    val projectSchema = SchemaUtils.project(schema, projectIndices)

    val predictSet = p match {
      case null => Set[Int]()
      case _ => p.leaves.map(_.colIndex).toSet
    }
    val projectMap = projectIndices.zipWithIndex.map(i => i._1 -> i._2).toMap

    val allColumnSet = predictSet.union(projectMap.keySet)

    val columnMap = allColumnSet.toList.sorted.zipWithIndex.map(f => f._1 -> f._2).toMap

    val nonPredictIndices = columnMap.filter(e => {
      !predictSet.contains(e._1)
    }).map(_._2).toList.sorted

    return (columnMap, projectMap, nonPredictIndices)
  }

  def readColumn(col: ColumnReaderImpl, valid: Boolean): Unit = {
    if (col.getCurrentDefinitionLevel == col.getDescriptor.getMaxDefinitionLevel) {
      valid match {
        case false => col.skip()
        case true => col.writeCurrentValueToConverter()
      }
    }
  }
}

class ColumnTempTablePipe(val table: ColumnTempTable, val index: Int) extends PredicatePipe {

  override def pipe(d: Double) = {
    table.add(index, d)
  }

  override def pipe(b: Binary) = {
    table.add(index, b)
  }

  override def pipe(i: Int) = {
    table.add(index, i)
  }

  override def pipe(l: Long) = {
    table.add(index, l)
  }

  override def pipe(bl: Boolean) = {
    table.add(index, bl)
  }

  override def pipe(f: Float) = {
    table.add(index, f)
  }
}

class RowTempTablePipe(val table: RowTempTable, val index: Int) extends PredicatePipe {

  override def pipe(d: Double) = {
    table.getConverter(index).asPrimitiveConverter().addDouble(d)
  }

  override def pipe(b: Binary) = {
    table.getConverter(index).asPrimitiveConverter().addBinary(b);
  }

  override def pipe(i: Int) = {
    table.getConverter(index).asPrimitiveConverter().addInt(i);
  }

  override def pipe(l: Long) = {
    table.getConverter(index).asPrimitiveConverter().addLong(l);
  }

  override def pipe(bl: Boolean) = {
    table.getConverter(index).asPrimitiveConverter().addBoolean(bl);
  }

  override def pipe(f: Float) = {
    table.getConverter(index).asPrimitiveConverter().addFloat(f);
  }
}

class VerticalSelect extends Select {
  override def select(input: URI, p: Predicate, schema: MessageType,
                      projectIndices: Array[Int]): ColumnTempTable = {

    val vp: VPredicate = p match {
      case null => null
      case _ => {
        p.leaves.zipWithIndex.foreach(p => {
          p._1.setType(schema.getType(p._2))
        })
        p.asInstanceOf[VPredicate]
      }
    }
    val recorder = createRecorder(SchemaUtils.project(schema, projectIndices))

    val (columnMap, projectMap, nonPredictIndices) = columnMapping(p, schema, projectIndices)

    ParquetReaderHelper.read(input, new EncReaderProcessor {
      override def processRowGroup(version: ParsedVersion,
                                   meta: BlockMetaData,
                                   rowGroup: PageReadStore): Unit = {
        val columns = schema.getColumns.zipWithIndex.filter(col => {
          columnMap.containsKey(col._2)
        }).map(col => {
          val converter = projectMap.getOrElse(col._2, -1) match {
            case -1 => new NonePrimitiveConverter
            case index => recorder.getConverter(index).asPrimitiveConverter()
          }
          new ColumnReaderImpl(col._1, rowGroup.getPageReader(col._1), converter, version)
        })

        if (vp != null) {
          vp.leaves.foreach(leaf => {
            leaf.setColumn(columns(columnMap.getOrElse(leaf.colIndex, -1)))
            // Install a pipe to push data that belongs to output columns
            if (projectMap.containsKey(leaf.colIndex)) {
              leaf.setPipe(new ColumnTempTablePipe(recorder, leaf.colIndex))
            }
          })

          val bitmap = vp.bitmap

          nonPredictIndices.map(columns(_)).foreach(col => {
            var counter = 0
            // Skip columns if necessary

            bitmap.foreach(stop => {
              while (counter < stop) {
                readColumn(col, false)
                col.consume()
                counter += 1
              }
              readColumn(col, true)
              col.consume()
              counter += 1
            })
            /*
            for (count <- 0L until rowGroup.getRowCount) {

              readColumn(col, bitmap.test(count))
              col.consume()
            }*/
          }
          )
        }

        else {
          nonPredictIndices.map(columns(_)).foreach(col => {
            var count = 0
            while (count < rowGroup.getRowCount) {
              readColumn(col, true)
              col.consume()
              count += 1
            }
          })
        }
      }
    }

    )

    return recorder
  }

  def createRecorder(schema: MessageType) = new ColumnTempTable(schema);
}

class HorizontalSelect extends Select {
  override def select(input: URI, p: Predicate, schema: MessageType,
                      projectIndices: Array[Int]): RowTempTable = {

    val hp: HPredicate = p match {
      case null => null
      case _ => {
        p.leaves.zipWithIndex.foreach(p => {
          p._1.setType(schema.getType(p._2))
        })
        p.asInstanceOf[HPredicate]
      }
    }
    val projectSchema = SchemaUtils.project(schema, projectIndices)
    val recorder = createRecorder(projectSchema)

    val (columnMap, projectMap, nonPredictIndices) = columnMapping(p, schema, projectIndices)

    ParquetReaderHelper.read(input, new EncReaderProcessor {
      override def processRowGroup(version: ParsedVersion,
                                   meta: BlockMetaData,
                                   rowGroup: PageReadStore): Unit = {
        val columns = schema.getColumns.zipWithIndex
          .filter(col => {
            columnMap.containsKey(col._2)
          })
          .map(col => {
            val converter = projectMap.getOrElse(col._2, -1) match {
              case -1 => new NonePrimitiveConverter
              case index => recorder.getConverter(index).asPrimitiveConverter()
            }
            new ColumnReaderImpl(col._1, rowGroup.getPageReader(col._1), converter, version)
          })

        if (hp != null) {
          hp.leaves.foreach(leaf => {
            leaf.setColumn(columns(columnMap.getOrElse(leaf.colIndex, -1)))
            if (projectMap.containsKey(leaf.colIndex)) {
              leaf.setPipe(new RowTempTablePipe(recorder, columnMap.getOrElse(leaf.colIndex, -1)));
            }
          })


          for (count <- 0L until rowGroup.getRowCount) {
            hp.value match {
              case true => {
                recorder.start()
                nonPredictIndices.map(columns(_)).foreach(col => {
                  readColumn(col, true)
                  col.consume()
                })
                recorder.end()
              }
              case _ => nonPredictIndices.map(columns(_)).foreach(col => {
                readColumn(col, false)
                col.consume()
              })
            }
          }
        } else {
          for (count <- 0L until rowGroup.getRowCount) {
            recorder.start()
            nonPredictIndices.map(columns(_)).foreach(col => {
              readColumn(col, true)
              col.consume()
            })
            recorder.end()
          }
        }
      }
    })
    return recorder
  }

  def createRecorder(schema: MessageType) = new RowTempTable(schema)
}
