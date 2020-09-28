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
package codecdb.dataset.column

import java.net.URI
import java.util

import codecdb.dataset.feature.Feature
import codecdb.model.DataType

import scala.collection.JavaConverters._

class Column(o: URI, ci: Int, cn: String, dt: DataType) extends Serializable {
  var origin: URI = o
  var colIndex: Int = ci
  var colName: String = cn
  var colFile: URI = _
  var dataType = dt
  var features: java.util.Set[Feature] = new util.HashSet[Feature]()
  var infos: java.util.Map[String, java.math.BigDecimal] = new util.HashMap[String, java.math.BigDecimal]();
  private var _parent: Column = null

  def parent: Column = _parent

  def parent_=(col: Column): Unit = {
    _parent = col
  }

  def this() {
    this(null, -1, null, null)
  }

  def findFeature(t: String, name: String): Option[Feature] = {
    features.asScala.find(f => f.featureType.equals(t) && f.name.equals(name))
  }

  def findFeatures(t: String): Iterable[Feature] = {
    features.asScala.filter(_.featureType.equals(t))
  }

  def replaceFeatures(fs: Iterable[Feature]) = {
    fs.foreach(f => {
      features.remove(f)
      features.add(f)
    })
  }

  def hasFeature(t: String): Boolean = features.asScala.exists(f => f.featureType.equals(t))

  def getInfo(n: String): Double = infos.getOrDefault(n, java.math.BigDecimal.valueOf(-1)).doubleValue()

  def putInfo(n: String, num: Double) = {
    infos.put(n, java.math.BigDecimal.valueOf(num))
  }
}