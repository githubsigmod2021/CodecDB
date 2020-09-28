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

import java.io.{File, FileOutputStream, PrintWriter}
import java.net.URI

import codecdb.dataset.column.Column
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import codecdb.ptnmining.matching.RegexMatcher
import codecdb.ptnmining.parser.{TSymbol, Tokenizer}
import codecdb.ptnmining.persist.JPAPatternPersistence
import codecdb.util.FileUtils
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence
import edu.uchicago.cs.encsel.ptnmining.parser.TSymbol

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Random

object MineColumn {
  val MAX_COLUMN = 15

  val patternMiner = new PatternMiner
  val matcher = RegexMatcher

  def mineColumn(column: Column): (Int, Boolean, String) = {
    val pattern = patternFromFile(column.colFile)
    val numc = numChildren(pattern)
    val valid = numc > 1 && numc <= MAX_COLUMN
    val persist = new JPAPersistence
    val patternStr = RegexMatcher.genRegex(pattern)

    if (valid) {
      cleanChildren(column)
      val newchildren = MineColumn.split(column, pattern)
      if (newchildren.nonEmpty) {
        persist.save(newchildren)
      }
      JPAPatternPersistence.save(column, patternStr)
    }
    (column match {
      case cw: ColumnWrapper => cw.id
      case _ => -100
    }, valid, patternStr)
  }

  def patternFromFile(file: URI): Pattern = {
    val nLine = FileUtils.numNonEmptyLine(file)
    val rate = 500.toDouble / nLine
    val source = Source.fromFile(file)
    try {
      val lines = source.getLines().map(_.trim).filter(_.nonEmpty).filter(p => {
        Random.nextDouble() <= rate
      })
      //    val tail = lines.takeRight(100)
      //    val both = head ++ tail
      val pattern = patternMiner.mine(lines.map(Tokenizer.tokenize(_).toSeq).toSeq)
      pattern.naming()

      pattern
    } finally {
      source.close
    }
  }

  def isSymbol(ptn: Pattern): Boolean = {
    ptn match {
      case token: PToken => token.token.isInstanceOf[TSymbol]
      case union: PUnion => {
        union.content.size == 2 && union.content.contains(PEmpty) &&
          union.content.exists(p => p.isInstanceOf[PToken] && p.asInstanceOf[PToken].token.isInstanceOf[TSymbol])
      }
      case _ => false
    }
  }

  def numChildren(pattern: Pattern): Int = {
    //    val validator = new PatternValidator
    //    pattern.visit(validator)
    //    validator.isValid match {
    //      case false => 0
    //      case true => {
    (pattern match {
      case seq: PSeq => {
        seq.content.flatMap(_ match {
          case union: PUnion => {
            if (isSymbol(union)) {
              None
            } else {
              Some(union)
            }
          }
          case any: PAny => Some(any)
          case _ => None
        })
      }
      case _ => Seq()
    }).size
    //      }
    //    }
  }

  def split(column: Column, pattern: Pattern): Seq[Column] = {
    if (column.dataType != DataType.STRING)
      throw new IllegalArgumentException()
    pattern.naming()
    pattern match {
      case seq: PSeq => {
        val colPatterns = seq.content.flatMap(_ match {
          case union: PUnion => Some(union)
          case any: PAny => Some(any)
          case _ => None
        })

        val colTypes = colPatterns.map(p => {
          typeof(p)
        }).toArray


        // columns for unmatched lines
        val unmatchCol = new Column(null, -1, "unmatch", DataType.STRING)
        unmatchCol.colFile = FileUtils.addExtension(column.colFile, "unmatch")
        unmatchCol.parent = column

        val outputs = colPatterns.indices.map(i =>
          new PrintWriter(new FileOutputStream(new File(FileUtils.addExtension(column.colFile, i.toString)))))
        val umoutput = new PrintWriter(new FileOutputStream(new File(unmatchCol.colFile)))

        val source = Source.fromFile(column.colFile)
        try {
          source.getLines().map(_.trim).foreach(line => {
            if (line.nonEmpty) {
              // Match the line against pattern
              val matched = matcher.matchon(pattern, line)
              if (matched.isDefined) {
                colPatterns.indices.foreach(i => {
                  val ptn = colPatterns(i)
                  colTypes(i) match {
                    case DataType.INTEGER | DataType.LONG => {
                      val value = matched.get.get(ptn.name)
                      if (value.isEmpty) {
                        outputs(i).println("")
                      } else {
                        val value = matched.get.get(ptn.name)
                        val iany = ptn.asInstanceOf[PIntAny]
                        val int = BigInt(value, if (iany.hasHex) 16 else 10)
                        if (int.toInt != int.toLong && colTypes(i) == DataType.INTEGER) {
                          colTypes(i) = DataType.LONG
                        }
                        outputs(i).println(int.toString)
                      }
                    }
                    case DataType.BOOLEAN => {
                      outputs(i).println(if (matched.get.get(ptn.name).isEmpty) 0 else 1)
                    }
                    case _ => {
                      outputs(i).println(matched.get.get(ptn.name))
                    }
                  }

                })
              } else {
                // Not match, write to unmatch
                umoutput.println(line)
              }
            } else {
              // Output empty line for empty line
              outputs.foreach(_.println(""))
            }
          })
          val childColumns = colPatterns.zipWithIndex.map(pi => {
            val col = new Column(null, pi._2, String.valueOf(pi._2), colTypes(pi._2))
            col.colFile = FileUtils.addExtension(column.colFile, pi._2.toString)
            col.parent = column
            col
          })

          childColumns :+ unmatchCol
        } finally {
          source.close
          outputs.foreach(_.close)
          umoutput.close
        }
      }
      case _ => Seq()
    }
  }

  def typeof(pattern: Pattern): DataType = {
    pattern match {
      case iany: PIntAny => DataType.INTEGER
      case dany: PDoubleAny => DataType.DOUBLE
      case union: PUnion => {
        if (isSymbol(union)) {
          DataType.BOOLEAN
        } else if (union.content.size == 2 && union.content.contains(PEmpty)) {
          typeof(union.content.filter(_ != PEmpty).head)
        } else
          DataType.STRING
      }
      case _ => DataType.STRING
    }
  }

  def cleanChildren(column: Column): Unit = {
    val persist = new JPAPersistence
    val sql = "SELECT c FROM Column c WHERE c.parentWrapper =:parent"
    val children = persist.em.createQuery(sql, classOf[ColumnWrapper])
      .setParameter("parent", column).getResultList.asScala
    if (children.nonEmpty) {
      persist.em.getTransaction.begin()
      children.foreach(c => {
        persist.em.remove(c)
      })
      persist.em.getTransaction.commit()
      children.foreach(c => {
        new File(c.colFile).delete()
      })
    }
  }

  def splitDouble(column: Column): Seq[Column] = {
    if (column.dataType != DataType.DOUBLE)
      throw new IllegalArgumentException()

    val outputs = (0 to 1).map(pi => FileUtils.addExtension(column.colFile, pi.toString)).toList
      .map(col => new PrintWriter(new FileOutputStream(new File(col))))
    val types = Array.fill(2)(DataType.INTEGER)

    val source = Source.fromFile(column.colFile)
    try {
      source.getLines().map(_.trim).foreach(line => {
        if (line.nonEmpty) {
          // Extract pieces
          val split = line.split("\\.")
          val data = (split.length match {
            case 2 => {
              (split(0), split(1))
            }
            case 1 => {
              (split(0), "0")
            }
            case _ => throw new IllegalArgumentException
          }).productIterator.toList
          // Update type
          for (i <- 0 to 1) {
            if (types(i) == DataType.INTEGER) {
              types(i) = try {
                data(i).toString.toInt
                DataType.INTEGER
              } catch {
                case e: NumberFormatException =>
                  DataType.LONG
              }
            }
          }
          outputs(0).println(data(0))
          outputs(1).println(data(1))
        } else {
          // Output empty line for empty line
          outputs.foreach(_.println(""))
        }
      })
      val childColumns = (0 to 1).map(pi => {
        val col = new Column(null, pi, String.valueOf(pi), types(pi))
        col.colFile = FileUtils.addExtension(column.colFile, pi.toString)
        col.parent = column
        col
      }).toList

      childColumns

    } finally {
      source.close
      outputs.foreach(_.close)
    }
  }
}

/**
  * There are currently several requirements to the pattern
  * 1. No too large unions
  * 2. No too long sequences
  *
  * Note: these rules are temporary and subject to change
  */
class PatternValidator extends PatternVisitor {

  var valid = true

  val unionThreshold = 50
  val seqThreshold = 15

  override def on(ptn: Pattern): Unit = {
    valid &= (ptn match {
      case union: PUnion => !path.isEmpty && union.content.size <= unionThreshold
      case seq: PSeq => seq.content.filter(!_.isInstanceOf[PToken]).size <= seqThreshold
      case _ => !path.isEmpty
    })
  }

  def isValid: Boolean = valid
}