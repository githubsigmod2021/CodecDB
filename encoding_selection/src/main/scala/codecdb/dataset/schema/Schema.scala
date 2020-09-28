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

import java.io.File
import java.io.FileOutputStream
import java.io.PrintWriter
import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths

import codecdb.model.DataType
import codecdb.util.FileUtils

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class Schema(var columns: Array[(DataType, String)], var hasHeader: Boolean = true) {
  def this() = {
    this(Array.empty[(DataType, String)])
  }
}

object Schema {
  def fromParquetFile(file: URI): Schema = {
    var hasheader = true
    var cols = new ArrayBuffer[(DataType, String)]()
    Source.fromFile(file).getLines().foreach {
      _ match {
        case hasheaderp(_*) => {
          hasheader = true
        }
        case noheaderp(_*) => {
          hasheader = false
        }
        case pattern(a, b) => {
          cols += ((dataType(a), b))
        }
        case _ => {}
      }
    }

    new Schema(cols.toArray, hasheader)
  }

  def toParquetFile(schema: Schema, file: URI): Unit = {
    val writer = new PrintWriter(new FileOutputStream(new File(file)))

    writer.println("message m {")

    if (schema.hasHeader) {
      writer.println("\thas_header")
    } else {
      writer.println("\tno_header")
    }
    schema.columns.foreach(col => {
      writer.println("\trequired\t%s\t%s;".format(col._1 match {
        case DataType.BOOLEAN => "boolean"
        case DataType.DOUBLE => "double"
        case DataType.FLOAT => "float"
        case DataType.INTEGER => "int32"
        case DataType.LONG => "int64"
        case DataType.STRING => "binary"
      }, col._2))
    })
    writer.println("}")

    writer.close()
  }

  private val pattern = "^\\s*(?:required|optional)\\s+([\\d\\w]+)\\s+([^\\s]+);\\s*$".r
  private val hasheaderp = "^\\s*has_header\\s*$".r
  private val noheaderp = "^\\s*no_header\\s*$".r

  private def dataType(parquetType: String): DataType = {
    parquetType match {
      case "int32" => DataType.INTEGER
      case "int64" => DataType.LONG
      case "binary" => DataType.STRING
      case "double" => DataType.DOUBLE
      case "float" => DataType.FLOAT
      case "boolean" => DataType.BOOLEAN
      case _ => throw new IllegalArgumentException(parquetType)
    }
  }

  def getSchema(source: URI): Schema = {
    // file_name + .schema
    var schemaUri = FileUtils.addExtension(source, "schema")
    if (new File(schemaUri).exists) {
      return Schema.fromParquetFile(schemaUri)
    }
    // file_name.abc => file_name.schema
    schemaUri = FileUtils.replaceExtension(source, "schema")
    if (new File(schemaUri).exists) {
      return Schema.fromParquetFile(schemaUri)
    }
    // file_name starts with schema
    val path = Paths.get(source)
    val pathname = path.getFileName.toString
    val schemas = Files.list(path.getParent).iterator().filter {
      p => {
        val pname = p.getFileName.toString
        pname.endsWith(".schema") && pathname.contains(pname.replace(".schema", ""))
      }
    }
    if (schemas.nonEmpty) {
      schemaUri = schemas.next().toUri
      return Schema.fromParquetFile(schemaUri)
    }
    null

  }

}
