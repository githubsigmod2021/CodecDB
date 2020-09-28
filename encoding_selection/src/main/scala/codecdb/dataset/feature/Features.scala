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

package codecdb.dataset.feature

import java.io._
import java.net.URI
import java.nio.file.{Files, Paths}

import codecdb.dataset.column.Column
import codecdb.dataset.feature.classify.{Distinct, Entropy, Length, Sortness, Sparsity}
import codecdb.dataset.feature.compress.ScanCompressedTimeUsage
import codecdb.dataset.feature.resource.ParquetEncFileSize
import edu.uchicago.cs.encsel.dataset.feature.classify._
import edu.uchicago.cs.encsel.dataset.feature.resource.ParquetEncFileSize
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Features {
  val logger = LoggerFactory.getLogger(getClass)
  val extractors = new ArrayBuffer[FeatureExtractor]()

    install(ParquetEncFileSize)
    install(Sparsity)
    install(Entropy)
    install(Length)
    install(Distinct)
    install(new Sortness(50))
    install(new Sortness(100))
    install(new Sortness(200))
//    install(AdjInvertPair)

//  install(ScanTimeUsage)
//  install(ScanCompressedTimeUsage)

  def install(fe: FeatureExtractor) = {
    extractors += fe
  }

  def extract(input: Column): Iterable[Feature] = {
    extractors.flatMap(ex => {
      try {
        ex.extract(input)
      } catch {
        case e: Exception => {
          logger.error("Exception while executing %s on %s:%s, skipping"
            .format(ex.getClass.getSimpleName, input.origin, input.colName), e)
          Iterable[Feature]()
        }
      }
    })
  }

  def extract(input: Column,
              filter: Iterator[String] => Iterator[String],
              prefix: String): Iterable[Feature] = {
    // Filter the file to URI
    val filteredURI = new URI(input.colFile.toString + "." + prefix)

    filterFile(input.colFile, filteredURI, filter)

    val filteredColumn = new Column(input.origin, input.colIndex, input.colName, input.dataType)
    filteredColumn.colFile = filteredURI

    val reader = new FileInputStream(new File(filteredURI))
    val buffer = new ByteArrayOutputStream()

    IOUtils.copy(reader, buffer)
    reader.close()
    buffer.close()

    val bufferArray = buffer.toByteArray

    val extracted = extractors.filter(_.supportFilter).flatMap(ex => {
      try {
        ex.extract(filteredColumn, new ByteArrayInputStream(bufferArray), prefix)
      } catch {
        case e: Exception => {
          logger.error("Exception while executing %s on %s:%s, skipping"
            .format(ex.getClass.getSimpleName, input.origin, input.colName), e)
          Iterable[Feature]()
        }
      }
    })
    // Delete the filtered file
    Files.delete(Paths.get(filteredURI))

    extracted
  }


  def filterFile(src: URI, target: URI, filter: Iterator[String] => Iterator[String]): Unit = {
    val filteredWriter = new PrintWriter(new FileOutputStream(new File(target)))
    val source = Source.fromFile(src)
    try {
      filter(source.getLines()).foreach(filteredWriter.println)
    } finally {
      source.close()
      filteredWriter.close()
    }
  }
}