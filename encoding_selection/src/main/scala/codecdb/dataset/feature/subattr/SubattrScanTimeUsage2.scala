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

package codecdb.dataset.feature.subattr

import java.io.InputStream
import java.nio.file.{Files, Paths}

import codecdb.dataset.column.Column
import codecdb.dataset.feature.{Feature, FeatureExtractor}
import codecdb.parquet.ParquetTupleReader
import codecdb.util.FileUtils
import codecdb.util.perf.Profiler
import edu.uchicago.cs.encsel.dataset.feature.FeatureExtractor
import org.slf4j.LoggerFactory

object SubattrScanTimeUsage2 extends FeatureExtractor {
  val logger = LoggerFactory.getLogger(getClass)

  def featureType = "SubattrScanTimeUsage"

  def supportFilter: Boolean = false

  val profiler = new Profiler

  def extract(col: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    val subtable = FileUtils.addExtension(col.colFile, "subtable")
    if (Files.exists(Paths.get(subtable))) {
      var counter = 0
      val originReader = new ParquetTupleReader(FileUtils.addExtension(col.colFile, "DICT_GZIP"))
      profiler.reset
      profiler.mark
      counter = 0
      while (counter < originReader.getNumOfRecords) {
        originReader.read()
        counter += 1
      }
      originReader.close()
      profiler.pause
      val otime = profiler.stop

      val tupleReader = new ParquetTupleReader(subtable)
      profiler.reset
      profiler.mark
      counter = 0
      while (counter < tupleReader.getNumOfRecords) {
        tupleReader.read()
        counter += 1
      }
      tupleReader.close()
      profiler.pause
      val time = profiler.stop
      Iterable(
        new Feature(featureType, "st_wallclock", time.wallclock),
        new Feature(featureType, "st_cpu", time.cpu),
        new Feature(featureType, "dg_wallclock", otime.wallclock),
        new Feature(featureType, "dg_cpu", otime.cpu)
      )
    } else {
      Iterable()
    }
  }

}
