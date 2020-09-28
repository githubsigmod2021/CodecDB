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

package codecdb.ptnmining.matching

import scala.collection.mutable.HashMap

/**
  * Mapping between pattern node and token
  */
class Record {
  private val values = new HashMap[String, String]

  def this(vs: Map[String, String]) {
    this()
    values ++= vs
  }

  val choices = new HashMap[String, (Int, Int)]

  // The delta towards min value of range
  val rangeDeltas = new HashMap[String, BigInt]

  def add(name: String, value: String) = {
    values += ((name, value))
  }

  def has(name: String): Boolean = values.contains(name)

  def get(name: String): String = values.getOrElse(name, "<err>")

}

