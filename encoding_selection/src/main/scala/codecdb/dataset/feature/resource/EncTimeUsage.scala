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

package codecdb.dataset.feature.resource

import java.io.InputStream
import java.lang.management.ManagementFactory

import codecdb.dataset.column.Column
import codecdb.dataset.feature.{Feature, FeatureExtractor}
import codecdb.model.{DataType, FloatEncoding, IntEncoding, LongEncoding, StringEncoding}
import codecdb.parquet.ParquetWriterHelper
import edu.uchicago.cs.encsel.dataset.feature.FeatureExtractor
import edu.uchicago.cs.encsel.model._
import org.slf4j.LoggerFactory

object EncTimeUsage extends FeatureExtractor {

  val logger = LoggerFactory.getLogger(getClass)

  def featureType = "EncTimeUsage"

  def supportFilter: Boolean = false

  def extract(col: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    val threadBean = ManagementFactory.getThreadMXBean
    // Ignore filter
    val fType = "%s%s".format(prefix, featureType)
    col.dataType match {
      case DataType.STRING => {
        StringEncoding.values().flatMap { e => {
          try {
            val startTime = System.currentTimeMillis()
            val startcpu = threadBean.getCurrentThreadCpuTime
            val f = ParquetWriterHelper.singleColumnString(col.colFile, e)
            val elapseTime = System.currentTimeMillis() - startTime
            val elapsecpu = threadBean.getCurrentThreadCpuTime - startcpu
            Iterable(
              new Feature(fType, "%s_wctime".format(e.name()), elapseTime),
              new Feature(fType, "%s_cputime".format(e.name()), elapsecpu)
            )
          } catch {
            case ile: IllegalArgumentException => {
              // Unsupported Encoding, ignore
              logger.warn("Exception when applying Encoding", ile.getMessage)
              Iterable()
            }
          }
        }
        }.filter(_ != null)
      }
      case DataType.LONG => {
        LongEncoding.values().flatMap { e => {
          try {
            val startTime = System.currentTimeMillis()
            val startcpu = threadBean.getCurrentThreadCpuTime
            val f = ParquetWriterHelper.singleColumnLong(col.colFile, e)
            val elapseTime = System.currentTimeMillis() - startTime
            val elapsecpu = threadBean.getCurrentThreadCpuTime - startcpu
            Iterable(
              new Feature(fType, "%s_wctime".format(e.name()), elapseTime),
              new Feature(fType, "%s_cputime".format(e.name()), elapsecpu)
            )
          } catch {
            case ile: IllegalArgumentException => {
              logger.warn("Exception when applying Encoding", ile.getMessage)
              Iterable()
            }
          }
        }
        }.filter(_ != null)
      }
      case DataType.INTEGER => {
        IntEncoding.values().flatMap { e => {
          try {
            val startTime = System.currentTimeMillis()
            val startcpu = threadBean.getCurrentThreadCpuTime
            val f = ParquetWriterHelper.singleColumnInt(col.colFile, e)
            val elapseTime = System.currentTimeMillis() - startTime
            val elapsecpu = threadBean.getCurrentThreadCpuTime - startcpu
            Iterable(
              new Feature(fType, "%s_wctime".format(e.name()), elapseTime),
              new Feature(fType, "%s_cputime".format(e.name()), elapsecpu)
            )
          } catch {
            case ile: IllegalArgumentException => {
              logger.warn("Exception when applying Encoding", ile.getMessage)
              Iterable()
            }
          }
        }
        }.filter(_ != null)
      }
      case DataType.FLOAT => {
        FloatEncoding.values().flatMap { e => {
          try {
            val startTime = System.currentTimeMillis()
            val startcpu = threadBean.getCurrentThreadCpuTime
            val f = ParquetWriterHelper.singleColumnFloat(col.colFile, e)
            val elapseTime = System.currentTimeMillis() - startTime
            val elapsecpu = threadBean.getCurrentThreadCpuTime - startcpu
            Iterable(
              new Feature(fType, "%s_wctime".format(e.name()), elapseTime),
              new Feature(fType, "%s_cputime".format(e.name()), elapsecpu)
            )
          } catch {
            case ile: IllegalArgumentException => {
              logger.warn("Exception when applying Encoding", ile.getMessage)
              Iterable()
            }
          }
        }
        }.filter(_ != null)
      }
      case DataType.DOUBLE => {
        FloatEncoding.values().flatMap { e => {
          try {
            val startTime = System.currentTimeMillis()
            val startcpu = threadBean.getCurrentThreadCpuTime
            val f = ParquetWriterHelper.singleColumnDouble(col.colFile, e)
            val elapseTime = System.currentTimeMillis() - startTime
            val elapsecpu = threadBean.getCurrentThreadCpuTime - startcpu
            Iterable(
              new Feature(fType, "%s_wctime".format(e.name()), elapseTime),
              new Feature(fType, "%s_cputime".format(e.name()), elapsecpu)
            )
          } catch {
            case ile: IllegalArgumentException => {
              logger.warn("Exception when applying Encoding", ile.getMessage)
              Iterable()
            }
          }
        }
        }.filter(_ != null)
      }
      case DataType.BOOLEAN => Iterable[Feature]() // Ignore BOOLEAN type
    }
  }
}
