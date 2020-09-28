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

package codecdb.query.tpch

import java.io.File

import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper
import ScanGenData.{intEncodings, stringEncodings}
import codecdb.parquet.{EncContext, ParquetWriterHelper}
import codecdb.query.VColumnPredicate
import codecdb.query.operator.VerticalSelect
import org.apache.parquet.column.Encoding
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConversions._

object Performance {

  val TPCH_FOLDER = "/home/harper/TPCH"

  def genInt(schema: MessageType, outputFolder: String, index: Int): Unit = {
    val inputFile = new File(TPCH_FOLDER + "/lineitem.tbl").toURI

    // Encode lineitem.line_number with different encodings
    intEncodings.foreach(intEncoding => {
      EncContext.encoding.get().put(schema.getColumns()(index).toString, intEncoding)

      ParquetWriterHelper.write(inputFile, schema,
        new File("%s/%s".format(outputFolder, intEncoding.name())).toURI, "\\|", false)
    })
  }

  def genString(schema: MessageType, outputFolder: String, index: Int): Unit = {
    val inputFile = new File(TPCH_FOLDER + "/lineitem.tbl").toURI

    // Encode lineitem.part_key with different encodings
    // Encode lineitem.line_number with different encodings
    stringEncodings.foreach(stringEncoding => {
      EncContext.encoding.get().put(schema.getColumns()(index).toString, stringEncoding)

      ParquetWriterHelper.write(inputFile, schema,
        new File("%s/%s".format(outputFolder, stringEncoding.name())).toURI, "\\|", false)
    })
  }

  def scan(schema: MessageType, outputPath: String, index: Int, predicate: Any => Boolean): Unit = {
    val folder = new File(outputPath)
    folder.listFiles().foreach(f => {
      try {
        Encoding.valueOf(f.getName)
        val start = System.currentTimeMillis()
        new VerticalSelect().select(f.toURI,
          new VColumnPredicate(predicate, index),
          schema,
          Array(index)
        )
        println("%s:%d".format(f.getName, System.currentTimeMillis() - start))
      } catch {
        case e: IllegalArgumentException => {}
      }
    })
  }
}
