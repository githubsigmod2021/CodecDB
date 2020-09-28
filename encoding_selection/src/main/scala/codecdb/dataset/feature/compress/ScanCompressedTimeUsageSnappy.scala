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

package codecdb.dataset.feature.compress

import java.io.{File, InputStream}
import java.net.URI

import codecdb.dataset.column.Column
import codecdb.dataset.feature.{Feature, FeatureExtractor}
import codecdb.query.operator.VerticalSelect
import codecdb.query.tpch.NostoreColumnTempTable
import codecdb.util.perf.Profiler
import edu.uchicago.cs.encsel.dataset.feature.FeatureExtractor
import codecdb.model.DataType._
import codecdb.model.{FloatEncoding, IntEncoding, LongEncoding, StringEncoding}
import codecdb.query.VColumnPredicate
import edu.uchicago.cs.encsel.model.LongEncoding
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}
import org.slf4j.LoggerFactory

object ScanCompressedTimeUsageSnappy extends FeatureExtractor {
  val logger = LoggerFactory.getLogger(getClass)

  def featureType = "ScanTimeUsage"

  def supportFilter: Boolean = false

  val predicate = new VColumnPredicate((data) => true, 0)
  val codecs = Array("SNAPPY")
  val profiler = new Profiler

  def encFunction(col: Column, encoding: String, schema: MessageType): Iterable[Feature] = {
    try {
      val select = new VerticalSelect() {
        override def createRecorder(schema: MessageType) = new NostoreColumnTempTable(schema)
      };

      val fileName = col.colFile + "." + encoding;
      val encfile = new URI(fileName)

      if (!new File(encfile).exists())
        return Iterable[Feature]()

      profiler.reset
      profiler.mark
      select.select(encfile, predicate, schema, Array(0))

      profiler.pause
      val time = profiler.stop

      Iterable(
        new Feature(featureType, "%s_wallclock".format(encoding), time.wallclock),
        new Feature(featureType, "%s_cpu".format(encoding), time.cpu),
        new Feature(featureType, "%s_user".format(encoding), time.user)
      )
    } catch {
      case e: Exception => {
        e.printStackTrace()
        Iterable[Feature]()
      }
    }
  }


  def extract(col: Column, input: InputStream, prefix: String): Iterable[Feature] = {

    if (col.hasFeature(ParquetCompressFileSize.featureType)) {
      col.dataType match {
        case INTEGER => {
          val schema = new MessageType("default",
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "value")
          )
          IntEncoding.values().filter(_.parquetEncoding() != null)
            .flatMap(encoding => {
              codecs.flatMap(codec => {
                encFunction(col, "%s_%s".format(encoding.name(), codec), schema)
              })
            })
        }
        case STRING => {
          val schema = new MessageType("default",
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "value")
          )
          StringEncoding.values().filter(_.parquetEncoding() != null)
            .flatMap(encoding =>
              codecs.flatMap(codec => {
                encFunction(col, "%s_%s".format(encoding.name(), codec), schema)
              })
            )
        }
        case DOUBLE => {
          val schema = new MessageType("default",
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "value")
          )
          FloatEncoding.values().filter(_.parquetEncoding() != null)
            .flatMap(encoding =>
              codecs.flatMap(codec => {
                encFunction(col, "%s_%s".format(encoding.name(), codec), schema)
              })
            )
        }
        case LONG => {
          val schema = new MessageType("default",
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "value")
          )
          LongEncoding.values().filter(_.parquetEncoding() != null)
            .flatMap(encoding =>
              codecs.flatMap(codec => {
                encFunction(col, "%s_%s".format(encoding.name(), codec), schema)
              })
            )
        }
        case BOOLEAN => {
          Iterable[Feature]()
        }
        case FLOAT => {
          val schema = new MessageType("default",
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, "value")
          )
          FloatEncoding.values().filter(_.parquetEncoding() != null)
            .flatMap(encoding =>
              codecs.flatMap(codec => {
                encFunction(col, "%s_%s".format(encoding.name(), codec), schema)
              })
            )
        }
      }
    } else {
      Iterable()
    }
  }
}
