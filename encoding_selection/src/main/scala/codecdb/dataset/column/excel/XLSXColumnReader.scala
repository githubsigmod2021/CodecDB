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
package codecdb.dataset.column.excel

import scala.collection.JavaConversions._
import org.apache.commons.csv.CSVRecord
import java.net.URI

import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileOutputStream
import java.io.PrintWriter

import codecdb.Config
import codecdb.dataset.column.{Column, ColumnReader}
import codecdb.dataset.parser.excel.XSSFRowRecord
import codecdb.dataset.schema.Schema
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFRow
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.xssf.usermodel.XSSFCell
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.DataFormatter

class XLSXColumnReader extends ColumnReader {

  def readColumn(source: URI, schema: Schema): Iterable[Column] = {

    fireStart(source)

    val tempFolder = allocTempFolder(source)
    val colWithWriter = schema.columns.zipWithIndex.map(d => {
      val col = new Column(source, d._2, d._1._2, d._1._1)
      col.colFile = allocFileForCol(tempFolder, d._1._2, d._2)
      val writer = new PrintWriter(new FileOutputStream(new File(col.colFile)))
      (col, writer)
    })

    val workbook = new XSSFWorkbook(new File(source))
    // By default only scan the first sheet
    val sheet = workbook.getSheetAt(0)
    val iterator = sheet.rowIterator()
    if (schema.hasHeader) {
      iterator.next()
    }
    iterator.foreach { record =>
      {
        fireReadRecord(source)
        val row = record.asInstanceOf[XSSFRow]
        if (!validate(row, schema)) {
          logger.warn("Malformated record in %s at %d found, skipping: %s"
            .format(source.toString, record.getRowNum, record.toString))
          fireFailRecord(source)
        } else {
          colWithWriter.foreach(pair => {
            val col = pair._1
            val writer = pair._2
            writer.println(XSSFRowRecord.content(row.getCell(col.colIndex)))
          })
        }
      }
    }
    colWithWriter.foreach(t => { t._2.close() })
    fireDone(source)
    colWithWriter.map(_._1)
  }

  def validate(record: XSSFRow, schema: Schema): Boolean = {
    if (!Config.columnReaderEnableCheck)
      return true

    schema.columns.zipWithIndex.foreach(col => {
      val cell = record.getCell(col._2)
      val datatype = col._1._1
      if (!datatype.check(XSSFRowRecord.content(cell)))
        return false
    })
    true
  }

}