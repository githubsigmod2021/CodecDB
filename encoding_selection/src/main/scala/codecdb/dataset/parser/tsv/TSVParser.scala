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
package codecdb.dataset.parser.tsv

import java.io.{BufferedInputStream, InputStream, InputStreamReader, Reader}

import State._
import codecdb.dataset.parser.{DefaultRecord, Parser, Record}
import codecdb.dataset.schema.Schema
import edu.uchicago.cs.encsel.dataset.parser.{DefaultRecord, Parser, Record}

import scala.collection.mutable.ArrayBuffer

/**
  * SimpleTSVParser is a line based parser and doesn't recognize double quote escape
  */
class SimpleTSVParser extends Parser {

  override def parseLine(line: String): Record = {
    line.trim.isEmpty match {
      case true => Record.EMPTY
      // Single tab,
      case false => new DefaultRecord(line.split("\t", -1))
    }
  }

  protected override def guessHeader(line: String): Unit = {
    guessedHeader = line.split("\t")
  }
}

/**
  * TSVParser supports double quotes escapes
  */
class TSVParser extends Parser {

  var reader: Reader = _

  override def parse(input: InputStream, schema: Schema): Iterator[Record] = {
    this.schema = schema

    val bufferedinput = new BufferedInputStream(input)
    // Guess Encoding
    val encoding = guessEncoding(bufferedinput)

    reader = new InputStreamReader(bufferedinput, encoding)
    if (schema == null || schema.hasHeader) {
      // Need to read out the first line
      var firstrec = readNextRecord()
      firstrec.isDefined match {
        case true => {
          if (schema == null) {
            guessedHeader = firstrec.get.iterator().toArray
          }
        }
        case false => {
          throw new IllegalArgumentException("No header found in file")
        }
      }
    }
    parseRecords()
  }

  protected def parseRecords(): Iterator[Record] =
    new Iterator[Record] {

      var nextRecord: Option[Record] = readNextRecord()

      override def hasNext: Boolean = nextRecord.isDefined

      override def next(): Record = {
        nextRecord.isDefined match {
          case true => {
            val toReturn = nextRecord.get
            nextRecord = readNextRecord()
            toReturn
          }
          case false => throw new IllegalStateException
        }
      }
    }

  var state = INIT

  // This method doesn't support escaping double quote or so for now
  // as I didn't find specification on how to escape it.
  protected def readNextRecord(): Option[Record] = {
    val buffer = new ArrayBuffer[String]()
    val fieldBuffer = new StringBuffer()
    var stop = false
    var eof = false
    while (!stop) {
      val nextInt = reader.read
      if (nextInt == -1) {
        stop = true
        eof = true
      } else {
        nextInt.toChar match {
          case '\n' => {
            state match {
              case INIT | ESCAPE => {
                state = INIT
                stop = true
              }
              case STRING => {
                fieldBuffer.append('\n')
              }
              case CRED => {
                state = INIT
              }
            }
          }
          case '\r' => {
            state match {
              case INIT | ESCAPE => {
                state = CRED
                stop = true
              }
              case STRING => {
                fieldBuffer.append('\r')
              }
              case CRED => {
                stop = true
              }
            }
          }
          case '\"' => {
            state match {
              case INIT | CRED => {
                state = STRING
              }
              case STRING => {
                state = ESCAPE
              }
              case ESCAPE => {
                fieldBuffer.append('\"')
                state = STRING
              }
            }
          }
          case '\t' => {
            state match {
              case INIT | CRED | ESCAPE => {
                buffer.append(fieldBuffer.toString)
                fieldBuffer.setLength(0)
                state = INIT
              }
              case STRING => {
                fieldBuffer.append('\t')
              }
            }
          }
          case c => {
            fieldBuffer.append(c)
            if (state == CRED)
              state = INIT
            if (state == ESCAPE) {
              throw new IllegalStateException("Not a valid escape character: %s".format(c.toString))
            }
          }
        }
      }
    }
    if (!(eof && fieldBuffer.length() == 0)) {
      buffer += fieldBuffer.toString
      fieldBuffer.setLength(0)
    }
    buffer.length match {
      case 0 => None
      case _ => Some(new DefaultRecord(buffer.toArray))
    }
  }
}

private object State {
  val INIT = 0
  val STRING = 1
  val CRED = 2
  val ESCAPE = 3
}