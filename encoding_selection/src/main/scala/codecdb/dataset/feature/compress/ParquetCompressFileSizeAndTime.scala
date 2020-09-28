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

import codecdb.dataset.column.Column
import codecdb.dataset.feature.{Feature, FeatureExtractor}
import codecdb.model.{DataType, FloatEncoding, IntEncoding, LongEncoding, StringEncoding}
import codecdb.parquet.ParquetCompressedWriterHelper
import codecdb.util.perf.Profiler
import edu.uchicago.cs.encsel.dataset.feature.FeatureExtractor
import edu.uchicago.cs.encsel.model._
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
  * Encode files using Parquet
  */
object ParquetCompressFileSizeAndTime extends FeatureExtractor {

  def featureType = ""

  def supportFilter: Boolean = false

  val codecs = Array(CompressionCodecName.SNAPPY, CompressionCodecName.GZIP, CompressionCodecName.LZO)
  val profiler = new Profiler


  def extract(col: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    // Ignore filter
    val fType = "%s%s".format(prefix, featureType)
    col.dataType match {
      case DataType.STRING => {
        StringEncoding.values().filter(_.parquetEncoding() != null).flatMap(e => {
          codecs.flatMap(codec => {
            try {
              val name = "%s_%s".format(e.name(), codec.name())
              profiler.reset
              profiler.mark
              val f = ParquetCompressedWriterHelper.singleColumnString(col.colFile, e, codec)
              profiler.pause
              val time = profiler.stop
              Iterable(
                new Feature(ParquetCompressFileSize.featureType, "%s_file_size".format(name), new File(f).length),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_wctime".format(name), time.wallclock),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_cputime".format(name), time.cpu),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_usertime".format(name), time.user)
              )
            } catch {
              case ile: IllegalArgumentException => {
                // Unsupported Encoding, ignore
                Iterable()
              }
            }
          })
        })
      }
      case DataType.LONG => {
        LongEncoding.values().filter(_.parquetEncoding() != null).flatMap(e => {
          codecs.flatMap(codec => {
            try {
              val name = "%s_%s".format(e.name(), codec.name())
              profiler.reset
              profiler.mark
              val f = ParquetCompressedWriterHelper.singleColumnLong(col.colFile, e, codec)
              profiler.pause
              val time = profiler.stop
              Iterable(
                new Feature(ParquetCompressFileSize.featureType, "%s_file_size".format(name), new File(f).length),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_wctime".format(name), time.wallclock),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_cputime".format(name), time.cpu),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_usertime".format(name), time.user)
              )
            } catch {
              case ile: IllegalArgumentException => {
                Iterable()
              }
            }
          })
        })
      }
      case DataType.INTEGER => {
        IntEncoding.values().filter(_.parquetEncoding() != null).flatMap(e => {
          codecs.flatMap(codec => {
            try {
              val name = "%s_%s".format(e.name(), codec.name())
              profiler.reset
              profiler.mark
              val f = ParquetCompressedWriterHelper.singleColumnInt(col.colFile, e, codec)
              profiler.pause
              val time = profiler.stop
              Iterable(
                new Feature(ParquetCompressFileSize.featureType, "%s_file_size".format(name), new File(f).length),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_wctime".format(name), time.wallclock),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_cputime".format(name), time.cpu),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_usertime".format(name), time.user)
              )
            } catch {
              case ile: IllegalArgumentException => {
                Iterable()
              }
            }
          })
        })
      }
      case DataType.FLOAT => {
        FloatEncoding.values().filter(_.parquetEncoding() != null).flatMap(e => {
          codecs.flatMap(codec => {
            try {
              val name = "%s_%s".format(e.name(), codec.name())
              profiler.reset
              profiler.mark
              val f = ParquetCompressedWriterHelper.singleColumnFloat(col.colFile, e, codec)
              profiler.pause
              val time = profiler.stop
              Iterable(
                new Feature(ParquetCompressFileSize.featureType, "%s_file_size".format(name), new File(f).length),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_wctime".format(name), time.wallclock),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_cputime".format(name), time.cpu),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_usertime".format(name), time.user)
              )
            } catch {
              case ile: IllegalArgumentException => {
                Iterable()
              }
            }
          })
        })
      }
      case DataType.DOUBLE => {
        FloatEncoding.values().filter(_.parquetEncoding() != null).flatMap(e => {
          codecs.flatMap(codec => {
            try {
              val name = "%s_%s".format(e.name(), codec.name())
              profiler.reset
              profiler.mark
              val f = ParquetCompressedWriterHelper.singleColumnDouble(col.colFile, e, codec)
              profiler.pause
              val time = profiler.stop
              Iterable(
                new Feature(ParquetCompressFileSize.featureType, "%s_file_size".format(name), new File(f).length),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_wctime".format(name), time.wallclock),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_cputime".format(name), time.cpu),
                new Feature(ParquetCompressTimeUsage.featureType, "%s_usertime".format(name), time.user)
              )
            } catch {
              case ile: IllegalArgumentException => {
                Iterable()
              }
            }
          })
        })
      }
      case DataType.BOOLEAN =>
        codecs.flatMap(codec => {
          val name = "PLAIN_%s".format(codec.name())
          profiler.reset
          profiler.mark
          val f = ParquetCompressedWriterHelper.singleColumnBoolean(col.colFile, codec)
          profiler.pause
          val time = profiler.stop
          Iterable(
            new Feature(ParquetCompressFileSize.featureType, "%s_file_size".format(name), new File(f).length),
            new Feature(ParquetCompressTimeUsage.featureType, "%s_wctime".format(name), time.wallclock),
            new Feature(ParquetCompressTimeUsage.featureType, "%s_cputime".format(name), time.cpu),
            new Feature(ParquetCompressTimeUsage.featureType, "%s_usertime".format(name), time.user)
          )
        })
    }
  }

}