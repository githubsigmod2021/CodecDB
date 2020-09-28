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
package codecdb.ptnmining

import java.io.{BufferedReader, File, FileReader}
import java.nio.file.{Files, Paths}

import codecdb.dataset.column.Column
import codecdb.dataset.feature.Feature
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.ptnmining.compose.PatternComposer
import codecdb.ptnmining.persist.PatternWrapper
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence
import codecdb.model.DataType._
import codecdb.model.{DataType, FloatEncoding, IntEncoding, LongEncoding, StringEncoding}
import codecdb.parquet.{EncContext, EncReaderProcessor, ParquetWriterBuilder}
import codecdb.util.FileUtils
import edu.uchicago.cs.encsel.model._
import edu.uchicago.cs.encsel.parquet.EncReaderProcessor
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.Encoding
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}

import scala.collection.JavaConverters._

object SubattrEncodeSingleFile extends App {

  val persist = new JPAPersistence()
  val em = persist.em
  val sql = "SELECT c FROM Column c WHERE EXISTS (SELECT p FROM Column p WHERE p.parentWrapper = c) AND c.id >= :start ORDER BY c.id"
  val childSql = "SELECT c FROM Column c WHERE c.parentWrapper = :parent"
  val patternSql = "SELECT p FROM Pattern p WHERE p.column = :col"
  val start = if (args.length == 0) 0 else args(0).toInt
  em.createQuery(sql, classOf[ColumnWrapper]).setParameter("start", start).getResultList.asScala.foreach(col => {
    println("Processing column %d".format(col.id))
    val children = getChildren(col)
    children.find(_.colName == "unmatch") match {
      case Some(um) => {
        val unmatchedLine = parquetLineCount(um)
        val total = parquetLineCount(col)
        val unmatchRate = unmatchedLine.toDouble / total
        col.replaceFeatures(Iterable(new Feature("SubattrStat", "unmatch_rate", unmatchRate)))
        persist.save(Iterable(col))
        // Build a single table
        val validChildren = children.filter(_.colName != "unmatch").sortBy(_.colIndex)
        val pattern = getPattern(col).pattern
        writeChildren(col, new PatternComposer(pattern), validChildren)
      }
      case None => {

      }
    }
  })

  def getChildren(col: Column): Seq[Column] = {
    em.createQuery(childSql, classOf[ColumnWrapper]).setParameter("parent", col).getResultList.asScala
  }

  def parquetLineCount(col: Column): Long = {
    //    val footer = ParquetFileReader.readFooter(new Configuration,
    //      new Path(col.colFile),
    //      ParquetMetadataConverter.NO_FILTER)
    //    footer.getBlocks.asScala.map(_.getRowCount).sum
    FileUtils.numLine(col.colFile)
  }

  def getPattern(col: Column) = {
    em.createQuery(patternSql, classOf[PatternWrapper]).setParameter("col", col).getSingleResult
  }

  def writeChildren(parent: Column, pattern: PatternComposer, children: Seq[Column]): Unit = {
    val file = FileUtils.addExtension(parent.colFile, "subtable")
    val path = Paths.get(file)
    if (Files.exists(path)) {
      Files.delete(path)
    }
    val optionalColumns = pattern.optionalColumns

    // Setup encoding for each column
    val schema = new MessageType("table",
      children.map(c => {
        val rep = Repetition.OPTIONAL// Always optional as entire line may be empty, which is unknown from header
        val typeName = c.dataType match {
          case INTEGER => INT32
          case STRING => BINARY
          case LONG => INT64
          case DataType.BOOLEAN => PrimitiveTypeName.BOOLEAN
          case DataType.DOUBLE => PrimitiveTypeName.DOUBLE
          case DataType.FLOAT => PrimitiveTypeName.FLOAT
        }
        new PrimitiveType(rep, typeName, c.colName)
      }): _*
    )

    schema.getColumns().toArray.zip(children).foreach(pair => {
      val cd = pair._1
      val child = pair._2
      val bestEnc = child.findFeatures("EncFileSize").filter(_.value > 0).minBy(_.value)
      val encName = bestEnc.name.replace("_file_size", "")

      val encoding = child.dataType match {
        case DataType.INTEGER => IntEncoding.valueOf(encName).parquetEncoding
        case DataType.STRING => StringEncoding.valueOf(encName).parquetEncoding
        case DataType.DOUBLE | DataType.FLOAT => FloatEncoding.valueOf(encName).parquetEncoding
        case DataType.LONG => LongEncoding.valueOf(encName).parquetEncoding
        case DataType.BOOLEAN => Encoding.RLE
      }
      // Fetch context from encoded file
      val encodedFile = FileUtils.addExtension(child.colFile, encName)


      EncContext.encoding.get().put(cd.toString, encoding)
      val context = EncReaderProcessor.getContext(encodedFile)(0).asInstanceOf[Array[AnyRef]]
      if (context != null)
        EncContext.context.get().put(cd.toString, context);
    })

    val writer = ParquetWriterBuilder.buildForTable(new Path(file), schema)

    val readers = children.map(c => {
      new BufferedReader(new FileReader(new File(c.colFile)))
    }).toList

    var valid = true
    while (valid) {
      val data = readers.map(_.readLine())
      valid = !data.exists(_ == null)
      if (valid)
        writer.write(data.asJava)
    }
    readers.foreach(_.close)
    writer.close()
  }
}
