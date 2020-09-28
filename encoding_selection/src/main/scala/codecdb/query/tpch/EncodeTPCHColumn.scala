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

package codecdb.query.tpch

import java.io.File
import java.util

import edu.uchicago.cs.encsel.model.FloatEncoding
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper
import ScanGenData.schema
import codecdb.model.{FloatEncoding, IntEncoding, StringEncoding}
import codecdb.parquet.{EncContext, ParquetCompressedWriterHelper, ParquetWriterHelper}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import scala.collection.JavaConverters._

object EncodeTPCHColumn extends App {

  val inputFile = new File(args(0))

  val fileName = inputFile.getName

  val schema = TPCHSchema.lineitemSchema

  val compressions = Array(CompressionCodecName.UNCOMPRESSED, CompressionCodecName.GZIP)
  //val compressions = Array(CompressionCodecName.SNAPPY)

  val context = new util.HashMap[String, Array[AnyRef]]()
  EncContext.context.set(context)
  // Set number of bits and int bound
  context.put(schema.getColumns().get(0).toString, Array("28", "180000000"))
  context.put(schema.getColumns().get(1).toString, Array("21", "2000000"))
  context.put(schema.getColumns().get(2).toString, Array("20", "300000"))
  context.put(schema.getColumns().get(3).toString, Array("3", "7"))
  context.put(schema.getColumns().get(4).toString, Array("7", "100"))
  schema.getColumns.asScala.zipWithIndex.foreach(c => {
    val colFile = new File(inputFile.getAbsolutePath + ".col" + String.valueOf(c._2 + 1)).toURI
    c._1.getType match {
      case PrimitiveTypeName.INT32 => {
        IntEncoding.values().toList.foreach(encoding => {
          compressions.foreach(codec => {
            val outputFile = ParquetWriterHelper.genOutput(colFile,
              String.format("%s_%s", encoding.name, codec.name))
            if (!outputFile.exists()) {
              try {
                ParquetCompressedWriterHelper.singleColumnInt(colFile, encoding, codec);
              } catch {
                case e: Exception => {
                  e.printStackTrace()
                }
              }
            }
          })
        })
      }
      case PrimitiveTypeName.BINARY => {
        StringEncoding.values().toList.foreach(encoding => {
          compressions.foreach(codec => {
            val outputFile = ParquetWriterHelper.genOutput(colFile,
              String.format("%s_%s", encoding.name, codec.name))
            if (!outputFile.exists()) {
              try {
                ParquetCompressedWriterHelper.singleColumnString(colFile, encoding, codec);
              } catch {
                case e: Exception => {
                  e.printStackTrace()
                }
              }
            }
          })
        })
      }
      case PrimitiveTypeName.DOUBLE => {
        FloatEncoding.values().toList.foreach(encoding => {
          compressions.foreach(codec => {
            val outputFile = ParquetWriterHelper.genOutput(colFile,
              String.format("%s_%s", encoding.name, codec.name))
            if (!outputFile.exists()) {
              try {
                ParquetCompressedWriterHelper
                  .singleColumnDouble(colFile, encoding, codec);
              } catch {
                case e: Exception => {
                  e.printStackTrace()
                }
              }
            }
          })
        })
      }
      case _ => {

      }
    }
  })
}
