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

package codecdb.dataset

import codecdb.dataset.column.Column
import codecdb.dataset.persist.Persistence
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.{DataType, FloatEncoding, IntEncoding, LongEncoding, StringEncoding}
import codecdb.parquet.ParquetWriterHelper
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence
import edu.uchicago.cs.encsel.model._

object EncodeColumn {
  def encode(col: Column) = {
    col.dataType match {
      case DataType.STRING => {
        StringEncoding.values().filter(_.parquetEncoding() != null).foreach(e =>
          try {
            ParquetWriterHelper.singleColumnString(col.colFile, e)
          } catch {
            case e: IllegalArgumentException => {
              e.printStackTrace()
            }
          }
        )
      }
      case DataType.LONG => {
        LongEncoding.values().filter(_.parquetEncoding() != null).foreach(e =>
          try {
            ParquetWriterHelper.singleColumnLong(col.colFile, e)
          } catch {
            case e: IllegalArgumentException => {
              e.printStackTrace()
            }
          }
        )
      }
      case DataType.INTEGER => {
        IntEncoding.values().filter(_.parquetEncoding() != null).foreach(e =>
          try {
            ParquetWriterHelper.singleColumnInt(col.colFile, e)
          } catch {
            case e: IllegalArgumentException => {
              e.printStackTrace()
            }
          }
        )
      }
      case DataType.FLOAT => {
        FloatEncoding.values().filter(_.parquetEncoding() != null).foreach(e =>
          try {
            ParquetWriterHelper.singleColumnFloat(col.colFile, e)
          } catch {
            case e: IllegalArgumentException => {
              e.printStackTrace()
            }
          }
        )
      }
      case DataType.DOUBLE => {
        FloatEncoding.values().filter(_.parquetEncoding() != null).foreach(e =>
          try {
            ParquetWriterHelper.singleColumnDouble(col.colFile, e)
          } catch {
            case e: IllegalArgumentException => {
              e.printStackTrace()
            }
          }
        )
      }
      case DataType.BOOLEAN => {}
    }
  }
}

object EncodeSingleColumn extends App {
  val start = args.length match {
    case 0 => throw new IllegalArgumentException
    case _ => args(0).toInt
  }
  EncodeColumn.encode(new JPAPersistence().find(start))
}

object EncodeAllColumns extends App {

  val start = args.length match {
    case 0 => 0
    case _ => args(0).toInt
  }
  val columns = Persistence.get.load()

  columns.foreach(col => {
    val colw = col.asInstanceOf[ColumnWrapper]
    println(colw.id)
    if (colw.id >= start)
      EncodeColumn.encode(colw)

  })
}
