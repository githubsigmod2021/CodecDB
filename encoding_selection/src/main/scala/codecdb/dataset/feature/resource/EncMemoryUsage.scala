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

package codecdb.dataset.feature.resource

import java.io.InputStream
import java.net.URI

import codecdb.dataset.column.Column
import codecdb.dataset.feature.{Feature, FeatureExtractor}
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.{DataType, FloatEncoding, IntEncoding, LongEncoding, StringEncoding}
import codecdb.parquet.ParquetWriterHelper
import edu.uchicago.cs.encsel.dataset.feature.FeatureExtractor
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence
import edu.uchicago.cs.encsel.model._
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.StringBuilderWriter
import org.slf4j.LoggerFactory

object EncMemoryUsage extends FeatureExtractor {
  val logger = LoggerFactory.getLogger(getClass)

  def featureType = "EncMemoryUsage"

  def supportFilter: Boolean = false

  def extract(col: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    // Ignore filter
    val fType = "%s%s".format(prefix, featureType)
    col.dataType match {
      case DataType.STRING => {
        StringEncoding.values().map { e => {
          new Feature(fType, "%s_maxheap".format(e.name()), executeAndMonitor(col, e.name()))
        }
        }
      }
      case DataType.LONG => {
        LongEncoding.values().map { e => {
          new Feature(fType, "%s_maxheap".format(e.name()), executeAndMonitor(col, e.name()))
        }
        }
      }
      case DataType.INTEGER => {
        IntEncoding.values().map { e => {
          new Feature(fType, "%s_maxheap".format(e.name()), executeAndMonitor(col, e.name()))
        }
        }
      }
      case DataType.FLOAT => {
        FloatEncoding.values().map { e => {
          new Feature(fType, "%s_maxheap".format(e.name()), executeAndMonitor(col, e.name()))
        }
        }
      }
      case DataType.DOUBLE => {
        FloatEncoding.values().map { e => {
          new Feature(fType, "%s_maxheap".format(e.name()), executeAndMonitor(col, e.name()))
        }
        }
      }

      case DataType.BOOLEAN => Iterable[Feature]() // Ignore BOOLEAN type
    }
  }

  /**
    * Get the memory usage of encoding this column with given encoding
    *
    * @param col
    * @param encoding
    * @return
    */
  def executeAndMonitor(col: Column, encoding: String): Long = {
    var maxMemory = 0l
    // Create Process
    val pb = new ProcessBuilder("/usr/bin/java",
      "-cp", "/local/hajiang/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar",
      "edu.uchicago.cs.encsel.dataset.feature.EncMemoryUsageProcess",
      col.colFile.toString, col.dataType.name(), encoding)
    //    pb.redirectErrorStream(true)
    val process = pb.start()

    process.waitFor();

    val buffer = new StringBuilderWriter()
    IOUtils.copy(process.getInputStream, buffer)

    buffer.close()
    val data = buffer.toString

    try {
      return data.trim.toInt
    } catch {
      case e: Exception => {
        return 0
      }
    }
  }
}

object EncMemoryUsageRun extends App {
  val colId = args(0).toInt
  val em = JPAPersistence.emf.createEntityManager()

  val col = em.createQuery("select c from Column c where c.id = :id", classOf[ColumnWrapper])
    .setParameter("id", colId).getSingleResult


  val memories = EncMemoryUsage.extract(col)

  memories.foreach(memory => {
    println(memory.name + " " + memory.value)
  })
}

/**
  * This is the main entry to load a column from database and
  * encode it using one encoding. Parent application will monitor the
  * memory usage using JMX and record the result
  */
object EncMemoryUsageProcess2 extends App {

  val colFile = new URI(args(0))
  val colDataType = DataType.valueOf(args(1))
  val encoding = args(2)

  colDataType match {
    case DataType.INTEGER => {
      val e = IntEncoding.valueOf(encoding)
      ParquetWriterHelper.singleColumnInt(colFile, e)
    }
    case DataType.LONG => {
      val e = LongEncoding.valueOf(encoding)
      ParquetWriterHelper.singleColumnLong(colFile, e)
    }
    case DataType.STRING => {
      val e = StringEncoding.valueOf(encoding)
      ParquetWriterHelper.singleColumnString(colFile, e)
    }
    case DataType.DOUBLE => {
      val e = FloatEncoding.valueOf(encoding)
      ParquetWriterHelper.singleColumnDouble(colFile, e)
    }
    case DataType.FLOAT => {
      val e = FloatEncoding.valueOf(encoding)
      ParquetWriterHelper.singleColumnFloat(colFile, e)
    }
    case _ => {

    }
  }
}