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
package codecdb.app.carbon

import java.io.{File, FileWriter, PrintWriter}
import java.net.URI
import java.nio.file.{Files, Paths}

import codecdb.Config
import codecdb.dataset.column.Column
import codecdb.dataset.persist.Persistence
import codecdb.dataset.persist.jpa.ColumnWrapper
import codecdb.model.DataType
import codecdb.parquet.{ParquetWriterBuilder, ParquetWriterHelper}
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet
import scala.io.Source
import scala.util.Try

object CompareDictAndBoolean extends App {

  val threshold = 20

  val cols = Persistence.get.load()
  cols.filter(col => {
    col.colName.toLowerCase().contains("category") && col.dataType == DataType.STRING && col.findFeature("Sparsity", "valid_ratio").get.value == 1
  }).foreach(col => {
    val distvals = distinct(col.colFile)
    if (distvals.size < threshold) {
      genColumn(distvals, col)
      genColumn2(distvals, col)
    }
  })

  def distinct(colFile: URI): Set[String] = {
    var values = new HashSet[String]()
    Source.fromFile(new File(colFile)).getLines().foreach(values += _.trim())
    values.toSet
  }

  def genColumn(distval: Set[String], column: Column) = {
    val folder = Files.createTempDirectory(Paths.get(Config.tempFolder),
      Try {
        column.asInstanceOf[ColumnWrapper].id.toString + "_"
      }.getOrElse(column.colName))
    val files = distval.map(value => {
      (value, folder.resolve(namize(value)).toFile)
    }).toMap
    val writers = files.map(kv => {
      (kv._1, new PrintWriter(new FileWriter(kv._2)))
    })

    Source.fromFile(new File(column.colFile)).getLines().foreach(line => {
      writers.foreach(writer => writer._2.println(line.trim() match {
        case writer._1 => "true"
        case _ => "false"
      }))
    })
    writers.foreach(_._2.close)

    val sumLength = files.toList.map(f => new File(ParquetWriterHelper.singleColumnBoolean(f._2.toURI)).length()).sum
    val dictLength = column.findFeature("EncFileSize", "DICT_file_size").get.value
    println(distval.size, sumLength, dictLength, sumLength / dictLength)
  }

  def genColumn2(distval: Set[String], column: Column) = {
    val folder = Files.createTempDirectory(Paths.get(Config.tempFolder),
      Try {
        column.asInstanceOf[ColumnWrapper].id.toString + "_"
      }.getOrElse(column.colName))
    val output = folder.resolve(column.colName)
    val row = distval.toList
    val schema = new MessageType("record",
      row.zipWithIndex.map(i => new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BOOLEAN, "value_2_%d".format(i._2))))

    val writer: ParquetWriter[java.util.List[String]] = ParquetWriterBuilder.buildDefault(new Path(output.toUri), schema)

    Source.fromFile(new File(column.colFile)).getLines().foreach(line =>
      writer.write(row.map { elem => line.equals(elem).toString }))

    writer.close()

    val sumLength = output.toFile.length
    val dictLength = column.findFeature("EncFileSize", "DICT_file_size").get.value
    println(distval.size, sumLength, dictLength, sumLength / dictLength)
  }

  def namize(input: String) = input.replaceAll("""[^\d\w]""", "_")
}

