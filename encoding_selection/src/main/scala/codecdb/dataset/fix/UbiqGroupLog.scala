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

import java.io.{File, PrintWriter}
import java.net.URI
import java.nio.file.{Files, Paths}

import com.google.gson.JsonParser

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Try

/**
  * The UbiqLog (see README.txt in dataset folder for detail) contains
  * several different types of logs. Separate them and store in different files
  */

// This formatter will merge incorrected separated lines and remove escape characters
class JsonFormatter(lines: Iterator[String]) extends Iterator[String] {

  protected val buffer: mutable.Buffer[String] = new ArrayBuffer[String]

  protected val look4DQ = """(?<!(\{|\"\s?\s?\s?[,:])\s?\s?\s?)\"(?!\s?\s?\s?[,:}])"""

  protected val jsonParser = new JsonParser

  def escape(input: String): String = {
    val nobs = input.replaceAll("\\\\", "\\\\\\\\")
    // Quick look for unescaped double quotes, any double quotes
    // not followed by ":" or "," will be escaped
    val nodq = nobs.replaceAll(look4DQ, "\\\\\"")
    nodq
  }

  def partJson(input: String): String = {
    var part = input
    while (!part.isEmpty && !Try {
      jsonParser.parse(part); true
    }.getOrElse(false)) {
      part = part.substring(1)
      val next = part.indexOf('{')
      next match {
        case -1 => part = ""
        case _ => part = part.substring(next)
      }
    }
    part.isEmpty match {
      case true => input
      case false => part
    }
  }

  def hasNext: Boolean = {
    while (lines.hasNext && (buffer.isEmpty || !buffer.last.endsWith("}"))) {
      buffer += lines.next()
    }
    if (!lines.hasNext && (buffer.isEmpty || buffer.last.endsWith("}"))) {
      return false
    }
    true
  }

  def next(): String = {
    if (buffer.isEmpty)
      throw new IllegalStateException
    val result = buffer.map(escape).mkString("\\n")
    buffer.clear()
    partJson(result)
  }
}

object GroupLog extends App {
  val root = "/local/hajiang/dataset/uci_repo/UbiqLog4UCI"
  //val root = "/Users/harper/ttt"

  val supportedEncodings = Array("utf-8", "iso-8859-1")
  val jsonParser = new JsonParser()
  val output = new mutable.HashMap[String, PrintWriter]

  scan(new File(root).toURI, process)

  output.values.foreach(_.close)


  def scan(folder: URI, proc: (URI, Int) => Unit): Unit = {
    Files.list(Paths.get(folder)).iterator().foreach(path => {
      if (path.toFile.isDirectory) {
        scan(path.toUri, proc)
      } else {
        val fname = path.getFileName.toString
        if (fname.startsWith("log") && fname.endsWith("txt")) {
          proc(path.toUri, 0)
        }
      }
    })
  }

  def process(uri: URI, encoding: Int): Unit = {
    try {
      new JsonFormatter(Source.fromFile(uri, supportedEncodings(encoding)).getLines())
        .foreach(line => {
          try {
            val json = jsonParser.parse(line).getAsJsonObject
            val entrySet = json.entrySet()
            entrySet.foreach(e => {
              val key = e.getKey
              val value = e.getValue
              output.getOrElseUpdate(key, new PrintWriter(root + "/" + key + ".json"))
                .println(value.toString)
            })
          } catch {
            case e: Exception => {
              println(e)
              println(line)
            }
          }
        })
    }
    catch {
      // Switch to next encoding
      case e: java.nio.charset.MalformedInputException => {
        println("Encoding error on " + uri.toString)
        process(uri, encoding + 1)
      }
    }
  }
}

