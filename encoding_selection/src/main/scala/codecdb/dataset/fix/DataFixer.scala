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

package codecdb.dataset.fix

import java.io.{File, FileOutputStream, PrintWriter}

import codecdb.dataset.column.Column
import codecdb.dataset.persist.Persistence
import codecdb.model.DataType

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * During a disk failure we notice some data are contaminated with symbols by unknown reason.
  * This tool tries to scan int/long/float/double data type and replace any symbols that are not digits
  *
  * String data is left unattended as we cannot distinguish correct data and contaminated data
  */
object DataFixer extends App {

  val sed = "sed -i '%ds/%s/%s/g' %s"

  fixAll


  def fixFile: Seq[String] = {
    val file = "/home/harper/pattern/demo"
    val column = new Column(null, 0, "", DataType.DOUBLE)
    column.colFile = new File(file).toURI

    fixColumn(column)
  }

  def fixAll: Unit = {
    val commands = new PrintWriter(new FileOutputStream("cmd_fix"))
    Persistence.get.load().foreach(col => {
      fixColumn(col).foreach(commands.println)
    })
    commands.close
  }

  def fixColumn(col: Column): Seq[String] = {
    val result = new ArrayBuffer[String]
    col.dataType match {
      case DataType.INTEGER | DataType.LONG => {
        var counter = 0
        var prevline: String = ""
        Source.fromFile(col.colFile).getLines().foreach(line => {
          counter += 1
          val trimed = line.trim
          if (!trimed.isEmpty) {
            try {
              BigInt(trimed)
              prevline = line
            } catch {
              case e: NumberFormatException => {
                // find the invalid char and generate command to replace data in this line
                val newline = prevline.length match {
                  case same if same == line.length => {
                    line.zip(prevline).map(pair => {
                      pair._1 match {
                        case digit if Character.isDigit(digit) => pair._1
                        case _ => pair._2
                      }
                    }).mkString
                  }
                  case _ => {
                    line.replaceAll("[:,;<>=\\?]", "0")
                  }
                }
                result += sed.format(counter, line, newline, new File(col.colFile).getAbsolutePath)
              }
            }
          }
        })
      }
      case DataType.FLOAT | DataType.DOUBLE => {
        var counter = 0
        var prevline: String = null
        Source.fromFile(col.colFile).getLines().foreach(line => {
          counter += 1
          val trimed = line.trim
          if (!trimed.isEmpty) {
            try {
              BigDecimal(trimed)
              prevline = line
            } catch {
              case e: NumberFormatException => {
                // find the invalid char and generate command to replace data in this line
                val newline = prevline.length match {
                  case same if same == line.length => {
                    line.zip(prevline).map(pair => {
                      pair._1 match {
                        case digit if Character.isDigit(digit) => digit
                        case _ => pair._2
                      }
                    }).mkString
                  }
                  case _ => {
                    line.replaceAll("[:;,<>=\\?]", "0")
                  }
                }
                result += sed.format(counter, line, newline, new File(col.colFile).getAbsolutePath)
              }
            }
          }
        })
      }
      case _ => Unit
    }
    result
  }
}
