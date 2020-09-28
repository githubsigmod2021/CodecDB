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
package codecdb.dataset.parser.excel

import java.io.File
import java.net.URI

import scala.collection.JavaConversions.asScalaIterator
import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.xssf.usermodel.XSSFCell
import org.apache.poi.xssf.usermodel.XSSFRow
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.InputStream

import codecdb.dataset.parser.{Parser, Record}
import codecdb.dataset.schema.Schema

class XLSXParser extends Parser {

  override def parse(input: InputStream, schema: Schema): Iterator[Record] = {
    this.schema = schema

    val workbook = new XSSFWorkbook(input)

    // By default only scan the first sheet
    val sheet = workbook.getSheetAt(0)

    val iterator = sheet.rowIterator()

    if (schema == null) {
      // Fetch a record to guess schema name
      val firstrec = iterator.next()
      guessedHeader = firstrec.iterator().map(c =>
        XSSFRowRecord.content(c.asInstanceOf[XSSFCell])).toArray
    } else if (schema.hasHeader) {
      // Skip first row as header
      iterator.next()
    }
    iterator.map { row => new XSSFRowRecord(row.asInstanceOf[XSSFRow]) }
  }

}

class XSSFRowRecord(row: XSSFRow) extends Record {
  val inner = row

  def apply(idx: Int): String = {
    XSSFRowRecord.content(inner.getCell(idx))
  }

  def length(): Int = {
    inner.getLastCellNum
  }

  override def toString: String = {
    "XSSFCell@%d[%s]".format(row.getRowNum,
      row.cellIterator().map { cell =>
        XSSFRowRecord
          .content(cell.asInstanceOf[XSSFCell])
      }.mkString(","))
  }

  def iterator(): Iterator[String] = {
    inner.cellIterator().map(c => XSSFRowRecord.content(c.asInstanceOf[XSSFCell]))
  }
}

object XSSFRowRecord {
  val formatter = new DataFormatter(true)

  def content(cell: XSSFCell): String = {
    if (null == cell)
      return ""
    formatter.formatCellValue(cell)
  }
}