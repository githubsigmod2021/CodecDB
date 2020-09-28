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

package codecdb.dataset.feature.classify

import java.io.InputStream

import codecdb.dataset.column.Column
import codecdb.dataset.feature.{Feature, FeatureExtractor}
import edu.uchicago.cs.encsel.dataset.feature.FeatureExtractor

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object AvgRunLength extends FeatureExtractor {

  override def featureType = "AvgRunLength"

  override def supportFilter = true

  def extract(column: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    val fType = "%s%s".format(prefix, featureType)
    val source = Source.fromInputStream(input)
    try {
      var rlength = new ArrayBuffer[Int];
      var current = "";
      var currentCount = 0;
      source.getLines().foreach(line => {
        if (current.equals(line)) {
          currentCount += 1;
        } else {
          if (currentCount != 0) {
            rlength += currentCount;
          }
          currentCount = 1;
          current = line;
        }
      });
      rlength += currentCount;
      val rlengthmean = rlength.sum.toDouble / rlength.size;

      Iterable(
        new Feature(fType, "value", rlengthmean))
    } finally {
      source.close()
    }
  }
}
