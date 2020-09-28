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

import java.io.{File, FileInputStream, InputStream}

import codecdb.dataset.column.Column

import scala.sys.process._

trait FeatureExtractor {

  def featureType: String

  def bufferData: Boolean = true

  protected def featureType(prefix: String): String = "%s%s".format(prefix, featureType)

  def supportFilter: Boolean

  def extract(input: Column, prefix: String = ""): Iterable[Feature] = {
    val is = bufferData match {
      case true => new FileInputStream(new File(input.colFile))
      case _ => null
    }
    try {
      extract(input, is, prefix)
    } finally {
      is.close()
    }
  }

  def extract(column: Column, input: InputStream, prefix: String): Iterable[Feature]
}

object FeatureExtractorUtils {

  def lineCount(column: Column): Int = {
    val result = "wc -l %s".format(column.colFile.getPath).!!
    result.trim.split("\\s+")(0).toInt
  }
}