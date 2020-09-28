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

import java.util

import Performance._
import codecdb.parquet.EncContext
import org.apache.parquet.column.Encoding

import scala.collection.JavaConversions._

// This program encode a single column with different encodings
object ScanGenData extends App {

  val intEncodings = Array(Encoding.PLAIN, Encoding.RLE, Encoding.BIT_PACKED,
    Encoding.PLAIN_DICTIONARY, Encoding.DELTA_BINARY_PACKED)

  val stringEncodings = Array(Encoding.PLAIN, Encoding.DELTA_BYTE_ARRAY,
    Encoding.DELTA_LENGTH_BYTE_ARRAY, Encoding.PLAIN_DICTIONARY)

  val schema = TPCHSchema.lineitemSchema

  // Encode lineitem.part_key with different encodings
  val encodings = new util.HashMap[String, Encoding]()
  EncContext.encoding.set(encodings)
  val context = new util.HashMap[String, Array[AnyRef]]()
  EncContext.context.set(context)

  // Set number of bits and int bound
  context.put(schema.getColumns()(1).toString, Array("18", "200000"))
  context.put(schema.getColumns()(3).toString, Array("3", "7"))

  genInt(schema, Performance.TPCH_FOLDER + "/scan_lineitem_partkey", 1)
  genInt(schema, Performance.TPCH_FOLDER + "/scan_lineitem_linenum", 3)
  genString(schema, Performance.TPCH_FOLDER + "/scan_lineitem_si", 13)
  genString(schema, Performance.TPCH_FOLDER + "/scan_lineitem_sm", 14)
  genString(schema, Performance.TPCH_FOLDER + "/scan_lineitem_comment", 15)

}

object ScanPerformanceTest extends App {
  val intEncodings = Array(Encoding.PLAIN, Encoding.RLE, Encoding.BIT_PACKED,
    Encoding.PLAIN_DICTIONARY, Encoding.DELTA_BINARY_PACKED)
  val schema = TPCHSchema.lineitemSchema

  val context = new util.HashMap[String, Array[AnyRef]]()
  EncContext.context.set(context)

  context.put(schema.getColumns()(1).toString, Array("18", "200000"))
  context.put(schema.getColumns()(3).toString, Array("3", "7"))

  scan(schema, Performance.TPCH_FOLDER + "/scan_lineitem_partkey", 1, (data: Any) => data.toString.toInt > 1250)
  scan(schema, Performance.TPCH_FOLDER + "/scan_lineitem_linenum", 3, (data: Any) => data.toString.toInt < 3)

  scan(schema, Performance.TPCH_FOLDER + "/scan_lineitem_si", 13, (data: Any) => true)
  scan(schema, Performance.TPCH_FOLDER + "/scan_lineitem_sm", 14, (data: Any) => true)
  scan(schema, Performance.TPCH_FOLDER + "/scan_lineitem_comment", 15, (data: Any) => true)
}