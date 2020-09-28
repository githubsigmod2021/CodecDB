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

import java.io.PrintWriter
import java.io.FileOutputStream
import java.io.File
import java.net.URI

import codecdb.Config
import codecdb.dataset.parser.{Parser, Record}
import codecdb.dataset.schema.Schema
import org.slf4j.LoggerFactory

class ParserColumnReader(p: Parser) extends ColumnReader {
  val parser = p

  def readColumn(source: URI, schema: Schema): Iterable[Column] = {

    fireStart(source)

    val tempFolder = allocTempFolder(source)
    val colWithWriter = schema.columns.zipWithIndex.map(d => {
      val col = new Column(source, d._2, d._1._2, d._1._1)
      col.colFile = allocFileForCol(tempFolder, d._1._2, d._2)
      val writer = new PrintWriter(new FileOutputStream(new File(col.colFile)))
      (col, writer)
    })

    var parsed = parser.parse(source, schema)

    if (schema.hasHeader)
      parsed = parsed.drop(1)
    parsed.foreach { row =>
      {
        fireReadRecord(source)
        if (!validate(row, schema)) {
          fireFailRecord(source)
          logger.warn("Malformated record in %s found, skipping: %s"
            .format(source.toString, row.toString()))
        } else {
          row.iterator().zipWithIndex.foreach(col => {
            colWithWriter(col._2)._2.println(col._1)
          })
        }
      }
    }
    colWithWriter.foreach(t => { t._2.close() })
    fireDone(source)

    colWithWriter.map(_._1)
  }

  def validate(record: Record, schema: Schema): Boolean = {
    if (!Config.columnReaderEnableCheck)
      return true
    if (record.length > schema.columns.length) {
      return false
    }
    schema.columns.zipWithIndex.foreach(col => {
      if (col._2 < record.length && !col._1._1.check(record(col._2)))
        return false
    })

    true
  }
}