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
package codecdb.dataset.column.csv

import java.io.File
import java.io.FileOutputStream
import java.io.FileReader
import java.io.PrintWriter
import java.net.URI

import codecdb.Config
import codecdb.dataset.column.{Column, ColumnReader}
import codecdb.dataset.schema.Schema

import scala.collection.JavaConversions.asScalaIterator
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVRecord
import org.slf4j.LoggerFactory

class CSVColumnReader2 extends ColumnReader {

  def readColumn(source: URI, schema: Schema): Iterable[Column] = {
    fireStart(source)
    val tempFolder = allocTempFolder(source)
    val colWithWriter = schema.columns.zipWithIndex.map(d => {
      val col = new Column(source, d._2, d._1._2, d._1._1)
      col.colFile = allocFileForCol(tempFolder, d._1._2, d._2)
      val writer = new PrintWriter(new FileOutputStream(new File(col.colFile)))
      (col, writer)
    })

    var parseFormat = CSVFormat.EXCEL
    if (schema.hasHeader)
      parseFormat = parseFormat.withFirstRecordAsHeader()
    else {
      parseFormat = parseFormat.withHeader(schema.columns.map(_._2): _*)
    }
    val parser = parseFormat.parse(new FileReader(new File(source)))


    val iterator = parser.iterator()
    //    if (schema.hasHeader) {
    //      iterator.next()
    //    }
    iterator.foreach { record => {
      fireReadRecord(source)
      if (!validate(record, schema)) {
        logger.warn("Malformated record at " + record.getRecordNumber + " found, skipping:" + record.toString)
        fireFailRecord(source)
      } else {
        record.iterator().zipWithIndex.foreach(rec => {
          colWithWriter(rec._2)._2.println(rec._1)
        })
      }
    }
    }
    colWithWriter.foreach(t => {
      t._2.close()
    })
    fireDone(source)
    colWithWriter.map(_._1)
  }

  def validate(record: CSVRecord, schema: Schema): Boolean = {
    if (!Config.columnReaderEnableCheck)
      return true
    if (record.size() > schema.columns.length) {
      return false
    }
    schema.columns.zipWithIndex.foreach(col => {
      if (col._2 < record.size && !col._1._1.check(record.get(col._2)))
        return false
    })

    true
  }
}