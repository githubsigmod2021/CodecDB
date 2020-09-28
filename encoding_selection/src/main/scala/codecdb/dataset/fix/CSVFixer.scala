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

package codecdb.dataset.fix

import java.io._
import java.net.URI

import org.apache.commons.csv.CSVFormat

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Fix malformated CSV files
  */


object FixCSV extends App {
  val output = new PrintWriter(new FileOutputStream(args(0) + ".fixed"))
  val bad = new PrintWriter(new FileOutputStream(args(0) + ".bad"))
  val fixcol = args.length match {
    case le if le <= 2 => -1
    case _ => args(2).toInt
  }
  val endWithDq = args.length match {
    case le if le <= 3 => true
    case _ => "true".equals(args(3))
  }
  new CSVFixer(args(1).toInt, fixcol, endWithDq).fix(new File(args(0)).toURI).foreach(p => {
    p._2 match {
      case true => output.println(p._1)
      case false => bad.println(p._1)
    }
  })
  output.close()
  bad.close()
}

/**
  * Filter data
  *
  * @param inner
  */
class FilterStream(val inner: InputStream) extends java.io.InputStream {
  override def read(): Int = {
    var char = inner.read()
    while (char > 127 || char == 0) {
      char = inner.read()
    }
    char
  }

  override def available(): Int = inner.available()
}

class CSVFixer(val expectColumn: Int, fixColumn: Int, endWithDq: Boolean) {

  val buffer = new ArrayBuffer[String]

  def fix(file: URI): Iterator[(String, Boolean)] = {
    Source.fromInputStream(new FilterStream(new FileInputStream(new File(file))), "utf-8")
      .getLines().map(line => {
      try {
        buffer += line
        if (endWithDq) {
          if (buffer.last.endsWith("\"")) {
            val tofix = buffer.mkString(" ")
            val fixed = new CSVLineFixer(expectColumn, fixColumn).fixLine(tofix)
            buffer.clear
            fixed.isEmpty match {
              case true => (tofix, false)
              case false => (fixed, true)
            }
          } else {
            ("", true)
          }
        } else {
          val fixed = new CSVLineFixer(expectColumn, fixColumn).fixLine(line)
          buffer.clear
          fixed.isEmpty match {
            case true => (line, false)
            case false => (fixed, true)
          }
        }
      } catch {
        case e: Exception => {
          System.err.println(line)
          throw e
        }
      }
    }).filter(p => !p._2 || !p._1.isEmpty)
  }

}

class CSVLineFixer(expectCol: Int, fixCol: Int) {

  val plain: (String) => String = { input =>
    try {
      val records = CSVFormat.EXCEL.parse(new StringReader(input)).getRecords
      val record = records(0)
      (record.size == expectCol) match {
        case true => input
        case false => ""
      }
    } catch {
      case e: Exception => {
        ""
      }
    }
  }

  val consecDoubleQuote = """"+""".r

  val escape: (String) => String = { input =>
    // It's hard to use regex, so just directly loop
    val buffer = new StringBuffer

    // Find match of double quotes, and escape those missed
    val seqmark = new ArrayBuffer[Int]
    val state = new mutable.Stack[Int]
    state.push(0)
    input.zipWithIndex.foreach(p => {
      val char = p._1
      val index = p._2
      char match {
        case '\"' => {
          state.head match {
            case 0 => {
              if (index == 0 || prev(input, index) == ',') {
                state.pop
                state.push(1)
                seqmark += index
              }
              if (next(input, index) == ',') {
                // Previous mark is wrong, remove it
                if (seqmark.nonEmpty)
                  seqmark.remove(seqmark.length - 1)
                seqmark += index
              }
            }
            case 1 => {
              if (index == input.length - 1 || next(input, index) == ',') {
                state.pop
                state.push(0)
                seqmark += index
              }
            }
            case 2 => Unit
          }
        }
        case '<' => {
          state.push(2)
        }
        case '>' => {
          state.pop
        }
        case _ => Unit
      }
    })
    val mark = new mutable.HashSet[Int]
    mark ++= seqmark

    consecDoubleQuote.findAllMatchIn(input).foreach(mch => {
      val range = (mch.start until mch.end)

      val unmarked = range.filter(i => !mark.contains(i))

      // Escape a odd block
      if (unmarked.size % 2 != 0) {
        mark ++= unmarked.drop(1)
      } else {
        mark ++= unmarked
      }
    })

    // Escape those not marked
    input.zipWithIndex.foreach(p => {
      val char = p._1
      val index = p._2
      buffer.append(char)
      if (char == '\"' && !mark.contains(index))
        buffer.append("\"")
    })

    try {
      val escaped = buffer.toString
      val records = CSVFormat.EXCEL.parse(new StringReader(escaped)).getRecords
      val record = records(0)
      (record.size == expectCol) match {
        case true => escaped
        case false => ""
      }
    } catch {
      case e: Exception => {
        ""
      }
    }
  }

  val col_merge: (String) => String = { input =>
    try {
      val records = CSVFormat.EXCEL.parse(new StringReader(input)).getRecords
      val record = records(0)
      (record.size == expectCol) match {
        case true => input
        case false => {
          fixCol match {
            case -1 => ""
            case _ => {
              //noinspection ReplaceToWithUntil
              // Merge additional columns to [length - fixCol - 1]
              val toMerge = expectCol - fixCol - 1 until record.size - fixCol
              val merged = "\"%s\"".format(toMerge.map(i => escapeString(record.get(i))).mkString(","))

              var combined = (0 until expectCol - fixCol - 1).map(i => escapeField(record.get(i))) :+ merged
              combined ++= (record.size - fixCol until record.size).map(i => escapeField(record.get(i)))

              val allMerged = combined.mkString(",")

              val records2 = CSVFormat.EXCEL.parse(new StringReader(allMerged)).getRecords
              val record2 = records2(0)
              (record2.size == expectCol) match {
                case true => allMerged
                case false => ""
              }
            }
          }
        }
      }
    } catch {
      case e: Exception => {
        ""
      }
    }
  }

  def prev(buffer: String, idx: Int): Char = {
    var pnt = idx - 1
    while (buffer(pnt) == ' ')
      pnt -= 1
    buffer.charAt(pnt)
  }

  def next(buffer: String, idx: Int): Char = {
    var pnt = idx + 1
    while (buffer(pnt) == ' ')
      pnt += 1
    buffer.charAt(pnt)
  }

  def escapeField(input: String): String = {
    input.contains(",") match {
      case true => "\"%s\"".format(escapeString(input))
      case false => input
    }
  }

  def escapeString(input: String): String = {
    input.replaceAll("\"", "\"\"")
  }

  val methods = Seq(plain, escape, col_merge)

  def fixLine(input: String): String = {
    var pointer = 0
    var found = false
    var res = ""
    while (pointer < methods.length && !found) {
      res = methods(pointer).apply(input)
      found = !res.isEmpty
      pointer += 1
    }
    res
  }


}

object State {
  val LINE_BEGIN = 0
  val REC_BEGIN = 1
  val REC_IN = 2
  val REC_END = 3
}