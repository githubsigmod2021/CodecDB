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
import codecdb.dataset.feature.compress.ParquetCompressFileSize
import codecdb.dataset.feature.resource.ParquetEncFileSize
import codecdb.ptnmining.compose.PatternComposer
import codecdb.ptnmining.persist.PatternWrapper
import javax.persistence.NoResultException
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence
import MineColumn._
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import codecdb.util.FileUtils

import scala.collection.JavaConverters._
import scala.io.Source
import scala.sys.process._

/**
  * This tool is used to mine from columns that were missing before
  * Constant: 19535 is the largest id for original columns
  */
object MineFixer extends App {

  val persist = new JPAPersistence
  booleanAsString

  def tooManyUnmatch: Unit = {
    val start = args.length match {
      case 0 => 0
      case _ => args(0).toInt
    }

    val loadcols = persist.em.createQuery("SELECT p FROM Column p WHERE p.dataType = :dt AND EXISTS (SELECT c FROM Column c WHERE c.parentWrapper = p)", classOf[ColumnWrapper])
      .setParameter("dt", DataType.STRING).getResultList

    loadcols.asScala.foreach(column => {
      // Check the unmatched file size, if non-zero, perform a rematch
      getUnmatch(column) match {
        case Some(unmatch) => {
          val numUnmatch = FileUtils.numLine(unmatch.colFile)
          if (numUnmatch != 0) {
            println("[Info ] column %d has error %d, regenerating".format(column.id, numUnmatch))
            mineColumn(column)
          }
        }
        case _ => {
          println("[Error] column %d has no unmatch".format(column.id))
        }
      }
    })
  }


  // Some (most) integer columns was encoded as long and will not be well encoded,
  // Find them and fix them
  def retypeIntColumnAndEncode: Unit = {
    val potColumns = persist.em.createQuery("SELECT c FROM Column c WHERE c.dataType = :dt AND c.parentWrapper IS NOT NULL", classOf[ColumnWrapper])
      .setParameter("dt", DataType.LONG).getResultList.asScala

    potColumns.foreach(col => {
      val source = Source.fromFile(col.colFile)
      val hasLong = source.getLines().filter(!_.isEmpty).exists(line => {
        val bint = BigInt(line)
        bint.toInt != bint.toLong
      })
      source.close()
      if (!hasLong) {
        println(col.id)
        col.dataType = DataType.INTEGER
        col.replaceFeatures(ParquetEncFileSize.extract(col))
        persist.save(Seq(col))
      }
    })
  }

  def booleanAsString: Unit = {
    val colsHasPattern = persist.em.createQuery("SELECT c FROM Column c WHERE c.parentWrapper IS NULL AND EXISTS (SELECT ptn FROM Pattern ptn WHERE ptn.column = c)", classOf[ColumnWrapper]).getResultList.asScala

    colsHasPattern.foreach(col => {
      val pattern = new PatternComposer(getPattern(col))
      val children = getChildren(col).filter(_.colIndex != -1)
      val checkFailed = pattern.booleanColumns.filter(i => children(i).dataType != DataType.BOOLEAN)
      if (checkFailed.nonEmpty) {
        // Replace a STRING column with BOOLEAN column
        checkFailed.foreach(i => {
          val child = children(i)
          // 1. Update type
          child.dataType = DataType.BOOLEAN
          // 2. Update column content
          var str = pattern.group(i)
          if("$.*/[\\]^".contains(str)) {
            str = "\\%s".format(str)
          }
          // Use sed to do in-place replacement
          val sedcmd = Seq("sed", "-i", "s/%s/1/g".format(str), new File(child.colFile).getAbsolutePath)
          val resp = sedcmd !!;
          if (resp.length() != 0)
            println("Error executing sed on %d".format(child.asInstanceOf[ColumnWrapper].id))
          else {
            // 3. Reset features
            child.features.clear()
            child.replaceFeatures(ParquetEncFileSize.extract(child))
            child.replaceFeatures(ParquetCompressFileSize.extract(child))
            persist.save(Iterable(child))
          }
        })
      }
    })
  }


  def getChildren(col: Column): Seq[Column] = {
    persist.em.createQuery("SELECT c FROM Column c WHERE c.parentWrapper = :p ORDER BY c.colIndex", classOf[ColumnWrapper])
      .setParameter("p", col).getResultList().asScala
  }

  def getPattern(col: Column): String = {
    persist.em.createQuery("SELECT p FROM Pattern p WHERE p.column = :c", classOf[PatternWrapper])
      .setParameter("c", col).getSingleResult.pattern
  }

  def getUnmatch(col: Column): Option[Column] = {
    val sql = "SELECT c FROM Column c WHERE c.parentWrapper = :parent AND c.colIndex = :idx"
    try {
      Some(persist.em.createQuery(sql, classOf[ColumnWrapper])
        .setParameter("parent", col)
        .setParameter("idx", -1)
        .getSingleResult)
    } catch {
      case e: NoResultException => None
    }
  }
}
