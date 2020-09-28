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

package codecdb.app.carbon

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path, Paths}

import codecdb.dataset.parser.csv.CommonsCSVParser
import codecdb.dataset.persist.Persistence
import codecdb.dataset.persist.jpa.JPAPersistence
import codecdb.dataset.schema.Schema
import codecdb.model.DataType

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.io.Source

object CompareWithCarbon extends App {

  val idfile = "/home/hajiang/carbon_id"
  val schema = new Schema(Array((DataType.STRING, "id")), false)
  val carbonRoot = "/home/hajiang/incubator-carbondata/bin/carbonshellstore/default"

  genReport()

  def compareSize(): Unit = {
    val parser = new CommonsCSVParser
    val records = parser.parse(new FileInputStream(idfile), schema)
    val carbondir = Paths.get(new File(carbonRoot).toURI)
    val persist = Persistence.get.asInstanceOf[JPAPersistence]

    records.foreach {
      rec => {
        val id = rec(0)
        val col = persist.find(id.toInt)
        val tabledir = carbondir.resolve("tab_" + id)
        val tablesize = folderSize(tabledir)
        val plainsize = col.findFeature("EncFileSize", "PLAIN_file_size").get.value.toInt
        val dictsize = col.findFeature("EncFileSize", "DICT_file_size").get.value.toInt
        val distinct = col.findFeature("Distinct", "Distinct").get.value.toInt

        println("%s,%d,%d,%d,%d".format(id, plainsize, dictsize, tablesize, distinct))
      }
    }
  }

  def folderSize(folder: Path): Long = {
    Files.walk(folder).iterator().filter {
      !Files.isDirectory(_)
    }.map(p => new File(p.toUri).length()).sum
  }

  def genReport() = {
    val dictData = new HashMap[(Int, Int), Int]
    val carbonData = new HashMap[(Int, Int), Int]

    Source.fromFile("/home/harper/enc_workspace/carbon_size").getLines().foreach(
      line => {
        val parts = line.split(",")
        val plain = parts(1).toInt
        val dict = parts(2).toInt
        val carbon = parts(3).toInt
        val distinct = parts(4).toInt

        val bin = (Math.round(Math.log10(dict.toDouble / carbon.toDouble))).toInt
        val distBin = Math.floor(Math.log10(distinct)).toInt match {
          case le2 if le2 <= 2 => 0
          case 3 => 1
          case 4 => 2
          case 5 => 3
          case _ => 4
        }

        val dictkey = (bin, distBin)

        dictData.put(dictkey, dictData.getOrElseUpdate(dictkey, 0) + 1)

      })

    println(dictData)
  }
}