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

package codecdb.ptnmining

import java.io.File

import codecdb.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence
import MineColumn._
import MineSingleColumn.column
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import codecdb.ptnmining.matching.GenRegexVisitor
import edu.uchicago.cs.encsel.ptnmining.matching.GenRegexVisitor
import edu.uchicago.cs.encsel.ptnmining.persist.PatternWrapper

import scala.collection.JavaConverters._

object MineAllColumns extends App {
  val start = args.length match {
    case 0 => 0
    case _ => args(0).toInt
  }

  val persist = new JPAPersistence

  val loadcols = persist.em.createQuery("SELECT c FROM Column c where c.id >= :id AND c.dataType = :dt AND c.parentWrapper IS NULL", classOf[ColumnWrapper]).setParameter("id", start).setParameter("dt", DataType.STRING).getResultList

  loadcols.asScala.foreach(col => {
    val result = mineColumn(col)
    println("%d:%s:%s".format(result._1, result._2, result._3))
  })
}

object MineSingleColumn extends App {
  val colid = args(0).toInt
  val column = new JPAPersistence().find(colid)

  val result = mineColumn(column)
  println("%d:%s:%s".format(result._1, result._2, result._3))
}

object MineSingleLocalFile extends App {
  val file = new File("/home/harper/pattern/test").toURI
  val pattern = patternFromFile(file)
  val numc = numChildren(pattern)
  val valid = numc > 1 && numc <= MAX_COLUMN
  val regex = new GenRegexVisitor
  pattern.visit(regex)
  println("%s:%s".format(regex.get, valid))
  if (valid) {
    val col = new Column(null, -1, "demo", DataType.STRING)
    col.colFile = file
    val subcols = MineColumn.split(col, pattern)
    println(subcols.size)
  }
}
