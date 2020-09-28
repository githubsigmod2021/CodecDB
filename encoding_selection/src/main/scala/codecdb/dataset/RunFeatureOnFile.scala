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

package codecdb.dataset

import java.io.File

import codecdb.dataset.column.Column
import codecdb.dataset.feature.compress.ParquetCompressTimeUsage
import codecdb.model.{DataType, StringEncoding}
import codecdb.parquet.ParquetCompressedWriterHelper
import codecdb.util.perf.Profiler
import edu.uchicago.cs.encsel.model.DataType
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.slf4j.LoggerFactory

object RunFeatureOnFile extends App {

  val logger = LoggerFactory.getLogger(getClass)

  val torun = ParquetCompressTimeUsage

  val file = new File(args(0)).toURI

  val encodings = Array(StringEncoding.PLAIN)
  val codecs = Array(CompressionCodecName.SNAPPY, CompressionCodecName.GZIP, CompressionCodecName.LZO)

  val column = new Column
  column.dataType = DataType.valueOf(args(1))
  column.colFile = file

  val profiler = new Profiler
  try
    codecs.foreach(f = codec => {
      profiler.reset
      profiler.mark
      ParquetCompressedWriterHelper.singleColumnString(file, encodings(0), codec)
      profiler.stop
      System.out.println("%s:%f".format(codec.name(), profiler.wcsum.asInstanceOf[Double]))
    })
  //    ParquetCompressTimeUsage.extract(column).foreach(f => {
  //      System.out.println("%s:%f".format(f.name, f.value))
  //    })
  catch {
    case e: Exception => {
      logger.warn("Failed during processing", e)
    }
  }

}
