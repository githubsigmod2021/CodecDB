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

package codecdb.query.simdscan

import java.io.File

import codecdb.parquet.{EncContext, ParquetWriterHelper}
import codecdb.query.tpch.TPCHSchema
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper
import org.apache.parquet.column.Encoding

object EncodeTPCHForSimdScan extends App {
  EncContext.encoding.get().put(
    TPCHSchema.lineitemSchema.getColumns().get(4).toString, Encoding.BIT_PACKED)
  EncContext.context.get().put(
    TPCHSchema.lineitemSchema.getColumns().get(4).toString, Array[AnyRef]("6", "50"))
  EncContext.encoding.get().put(
    TPCHSchema.lineitemSchema.getColumns().get(5).toString, Encoding.RLE_DICTIONARY
  )
  EncContext.encoding.get().put(
    TPCHSchema.lineitemSchema.getColumns().get(6).toString, Encoding.RLE_DICTIONARY
  )
  EncContext.encoding.get().put(
    TPCHSchema.lineitemSchema.getColumns().get(7).toString, Encoding.RLE_DICTIONARY
  )
  EncContext.encoding.get().put(
    TPCHSchema.lineitemSchema.getColumns().get(9).toString, Encoding.RLE_DICTIONARY
  )
  EncContext.encoding.get().put(
    TPCHSchema.lineitemSchema.getColumns().get(10).toString, Encoding.RLE_DICTIONARY
  )
  ParquetWriterHelper.write(new File(args(0)).toURI, TPCHSchema.lineitemSchema,
    new File(args(1)).toURI, "\\|", false)
}
