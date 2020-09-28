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

package codecdb.report

import java.io.{File, FileOutputStream, PrintWriter}

import codecdb.dataset.column.Column
import codecdb.dataset.feature.resource.ParquetEncFileSize
import codecdb.model.DataType

import scala.io.Source

object EncodeTPCHFilesAndCheckSize extends App {

//  readLineItem
  encodeLineItem

  def encodeLineItem: Unit = {
    val index = Array((4, DataType.INTEGER), (5, DataType.DOUBLE), (6, DataType.DOUBLE), (10, DataType.STRING))
    index.foreach(i => {
      val col = new Column(null, 0, "", i._2)
      col.colFile = new File("/home/harper/TPCH/lineitem." + i._1).toURI

      val features = ParquetEncFileSize.extract(col)
      val plain = features.find(p => p.name == "PLAIN_file_size").get
      val dict = features.find(p => p.name == "DICT_file_size").get
      val best = features.filter(_.value > 0).minBy(_.value)

      println(Seq(plain, dict, best).map(_.value).mkString(","))
    })
  }

  def encodePart: Unit = {

    val index = Array((0, DataType.INTEGER), (4, DataType.STRING))
    index.foreach(i => {
      val col = new Column(null, 0, "", i._2)
      col.colFile = new File("/home/harper/TPCH/part." + i._1).toURI

      val features = ParquetEncFileSize.extract(col)
      val plain = features.find(p => p.name == "PLAIN_file_size").get
      val dict = features.find(p => p.name == "DICT_file_size").get
      val best = features.filter(_.value > 0).minBy(_.value)

      println(Seq(plain, dict, best).map(_.value).mkString(","))
    })
  }

  // Split TPCH Files into separate columns
  def readLineItem: Unit = {
    val index = Array(1, 4, 5, 6, 10)
    val output = index.map(i => new PrintWriter(new FileOutputStream("/home/harper/TPCH/lineitem." + i.toString))).toList

    val lineitemInput = Source.fromFile("/home/harper/TPCH/lineitem.tbl")
      .getLines().foreach(line => {
      val pieces = line.split("\\|")

      index.zipWithIndex.foreach(p => {
        output(p._2).println(pieces(p._1))
      })
    })
    output.foreach(_.close)
  }

  def readPart: Unit = {
    val index = Array(0, 4)
    val output = index.map(i => new PrintWriter(new FileOutputStream("/home/harper/TPCH/part." + i.toString))).toList

    val lineitemInput = Source.fromFile("/home/harper/TPCH/part.tbl")
      .getLines().foreach(line => {
      val pieces = line.split("\\|")

      index.zipWithIndex.foreach(p => {
        output(p._2).println(pieces(p._1))
      })
    })
    output.foreach(_.close)
  }
}
