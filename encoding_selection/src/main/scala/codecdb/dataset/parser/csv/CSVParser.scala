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
package codecdb.dataset.parser.csv

import codecdb.dataset.parser.{DefaultRecord, Parser, Record}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class CSVParser extends Parser {

  override protected def parseLine(line: String): Record = {
    var content = new ArrayBuffer[String]()
    val buffer = new StringBuffer()
    var state = 0 // 0 is field start, 1 is in string, 2 is in field, 3 is string end, 4 is waiting double quote ""
    line.foreach { c =>
      {
        state match {
          case 0 => {
            c match {
              case '\"' => { state = 1 }
              case ',' => { content += buffer.toString; buffer.delete(0, buffer.length()) }
              case _ => { state = 2; buffer.append(c) }
            }
          }
          case 1 => {
            c match {
              case '\"' => { state = 3 }
              case _ => { buffer.append(c) }
            }
          }
          case 2 => {
            c match {
              case ',' => { content += buffer.toString; buffer.delete(0, buffer.length()); state = 0 }
              case _ => { buffer.append(c) }
            }
          }
          case 3 => {
            c match {
              case '\"' => { buffer.append(c); state = 1 }
              case ',' => { content += buffer.toString; buffer.delete(0, buffer.length()); state = 0 }
              case _ => throw new IllegalArgumentException("" + c)
            }
          }
          case _ => throw new IllegalArgumentException()
        }
      }
    }
    if (state == 0 || state == 2 || state == 3) {
      content += buffer.toString
    }
    new DefaultRecord(content.toArray)
  }
}