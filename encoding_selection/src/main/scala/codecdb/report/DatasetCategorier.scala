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

import java.io.File
import java.net.URI

import codecdb.dataset.column.Column
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DatasetCategorier extends App {

  val persist = new JPAPersistence

  val cols = persist.em.createQuery("SELECT col FROM Column col WHERE col.parentWrapper IS NULL", classOf[ColumnWrapper])
    .getResultList.asScala

  val map = new mutable.HashMap[String, mutable.Buffer[Column]]
  val remain = new mutable.HashSet[String]

  cols.foreach(col => {
    if (col.origin.toString.contains("argonne")) {
      map.getOrElseUpdate("NationalLab", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("uci")) {
      map.getOrElseUpdate("Machine Learning", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("gis")) {
      map.getOrElseUpdate("GIS", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("bike")) {
      map.getOrElseUpdate("GIS", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("bike")) {
      map.getOrElseUpdate("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("taxi")) {
      map.getOrElseUpdate("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("911")) {
      map.getOrElseUpdate("Government", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("311")) {
      map.getOrElseUpdate("Government", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("tree")) {
      map.getOrElseUpdate("Government", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("tax")) {
      map.getOrElseUpdate("Financial", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("payment")) {
      map.getOrElseUpdate("Financial", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("permit")) {
      map.getOrElseUpdate("Government", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("fire")) {
      map.getOrElseUpdate("Government", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("vem")) {
      map.getOrElseUpdate("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("parking")) {
      map.getOrElseUpdate("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("mv")) {
      map.getOrElseUpdate("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("pv")) {
      map.getOrElseUpdate("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("speed")) {
      map.getOrElseUpdate("Traffic", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("yelp")) {
      map.getOrElseUpdate("Social Network", new ArrayBuffer[Column]) += col
    } else if (col.origin.toString.contains("dp")) {
      map.getOrElseUpdate("Government", new ArrayBuffer[Column]) += col
    } else {
      map.getOrElseUpdate("Other", new ArrayBuffer[Column]) += col
    }
  })

  map.foreach(stat)


  // Number of tables, number of columns, raw data size
  def stat(entry: (String, Seq[Column])): Unit = {
    val numTables = entry._2.map(_.origin.toString).toSet.size
    val numColumns = entry._2.size
    val rawDataSize = entry._2.map(_.origin.toString).distinct.map(str => new File(new URI(str)).length).sum

    println("%s & %d, %d, %d".format(entry._1, numTables, numColumns, rawDataSize))
  }
}
