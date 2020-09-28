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

class Feature(t: String) extends Serializable {

  var featureType: String = t
  var name: String = _
  var value: Double = -1

  def this() {
    this(null)
  }

  def this(t: String, n: String, v: Double) {
    this(t)
    this.name = n
    this.value = v
  }

  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[Feature]) {
      val fea = obj.asInstanceOf[Feature]
      return this.featureType.equals(fea.featureType) && this.name.equals(fea.name)
    }
    super.equals(obj)
  }

  override def hashCode(): Int = {
    13 * this.featureType.hashCode() + this.name.hashCode()
  }
}