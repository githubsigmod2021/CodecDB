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
package codecdb.dataset.schema

import java.net.URI
import java.text.NumberFormat

import codecdb.dataset.parser.ParserFactory
import codecdb.model.DataType
import org.slf4j.LoggerFactory

import scala.util.Try

class SchemaGuesser {

  val logger = LoggerFactory.getLogger(getClass)

  def guessSchema(file: URI): Schema = {
    val parser = ParserFactory.getParser(file)
    if (null == parser) {
      if (logger.isDebugEnabled())
        logger.debug("No parser available for %s".format(file.toString))
      return null
    }
    val records = parser.parse(file, null)

    val guessedHeader = parser.guessHeaderName
    val columns = guessedHeader.map(_.replaceAll("[^\\d\\w_]+", "_"))
      .map((DataType.BOOLEAN, _))

    var malformatCount = 0
    records.foreach(record => {
      if (record.length == columns.length) {
        for (i <- columns.indices) {
          var value = record(i)
          if (value != null && value.trim().length() != 0) {
            value = value.trim()

            val expected = testType(value, columns(i)._1)
            if (expected != columns(i)._1)
              columns(i) = (expected, columns(i)._2)
          }
        }
      } else {
        malformatCount += 1
      }
    })
    if (malformatCount > 0) {
      logger.warn("Malformatted record counts %d in %s".format(malformatCount, file.toString))
    }
    new Schema(columns, true)
  }

  protected val booleanValues = Set("0", "1", "yes", "no", "true", "false")
  protected val numberRegex = """[\-]?[\d,]+""".r
  protected val floatRegex = """[\-]?[,\d]*(\.\d*)?(E\d*)?""".r

  protected val numberParser = NumberFormat.getInstance

  def testType(input: String, expected: DataType): DataType = {
    expected match {
      case DataType.BOOLEAN => {
        if (booleanValues.contains(input.toLowerCase()))
          DataType.BOOLEAN
        else
          testType(input, DataType.INTEGER)
      }
      case DataType.INTEGER => {
        input match {
          case numberRegex(_*) => {
            Try {
              val num = numberParser.parse(input)
              num match {
                case x if x.longValue() != x.doubleValue() => DataType.STRING // Too Long
                case x if x.intValue() == x.longValue() => DataType.INTEGER
                case _ => DataType.LONG
              }
            }.getOrElse(DataType.STRING)
          }
          case floatRegex(_*) => testType(input, DataType.DOUBLE)
          case _ => DataType.STRING
        }
      }
      case DataType.LONG => {
        input match {
          case numberRegex(_*) => {
            Try {
              val num = numberParser.parse(input)
              num match {
                case x if x.longValue() != x.doubleValue() => DataType.STRING // Too Long
                case _ => DataType.LONG
              }
            }.getOrElse(DataType.STRING)
          }
          case floatRegex(_*) => testType(input, DataType.DOUBLE)
          case _ => DataType.STRING
        }
      }
      case DataType.FLOAT => {
        input match {
          case floatRegex(_*) =>
            Try {
              val num = numberParser.parse(input)
              num match {
                case x if x.floatValue() == x.doubleValue() => DataType.FLOAT
                case _ => DataType.DOUBLE
              }
            }.getOrElse(DataType.STRING)
          case _ => DataType.STRING
        }
      }
      case DataType.DOUBLE => {
        input match {
          case floatRegex(_*) =>
            Try {
              val num = numberParser.parse(input)
              DataType.DOUBLE
            }.getOrElse(DataType.STRING)
          case _ => DataType.STRING
        }
      }
      case DataType.STRING => expected
    }
  }
}