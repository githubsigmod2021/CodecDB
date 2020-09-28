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
package codecdb.dataset.column

import java.net.URI

import codecdb.dataset.column.csv.{CSVColumnReader, CSVColumnReader2}
import codecdb.dataset.column.excel.XLSXColumnReader
import codecdb.dataset.column.json.JsonColumnReader
import codecdb.dataset.column.tsv.TSVColumnReader

object ColumnReaderFactory {

  def getColumnReader(source: URI): ColumnReader = {
    source.getScheme match {
      case "file" => {
        source.getPath match {
          case x if x.toLowerCase().endsWith("csv") => {
            new CSVColumnReader2
          }
          case x if x.toLowerCase().endsWith("tsv") => {
            new TSVColumnReader
          }
          case x if x.toLowerCase().endsWith("json") => {
            new JsonColumnReader
          }
          case x if x.toLowerCase().endsWith("xlsx") => {
            new XLSXColumnReader
          }
          case _ =>
            null
        }
      }
      case _ =>
        null
    }
  }
}

object DataSource extends Enumeration {
  type Type = Value
  val CSV, TSV = Value
}