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

package codecdb.util

import org.apache.commons.lang.StringUtils
import org.nd4j.linalg.api.ndarray.INDArray
import scala.collection.mutable.ArrayBuffer
import org.nd4j.linalg.ops.transforms.Transforms

object WordUtils {

  def levDistance(a: String, b: String): Int = {
    val matrix = Array[Array[Int]]((0 to a.length).map(_ => new Array[Int](b.length + 1)): _*)
    matrix(0)(0) = 0
    for (i <- 1 to a.length) matrix(i)(0) = i
    for (j <- 1 to b.length) matrix(0)(j) = j

    for (j <- 1 to b.length; i <- 1 to a.length) {
      val substcost = (a(i - 1) == b(j - 1)) match {
        case true => 0
        case _ => 1
      }
      matrix(i)(j) = Array(matrix(i)(j - 1) + 1, matrix(i - 1)(j) + 1, matrix(i - 1)(j - 1) + substcost).min
    }

    matrix(a.length)(b.length)
  }
  /**
   * Add a weight to lev distance, a difference at position p has a difference 1.15 - tanh (0.15p)
   * This has an effort that the first char has distance 1, the 5th has distance 0.5 and chars after 10 has 0.15
   */
  def levDistance2(a: String, b: String): Double = {
    val matrix = Array[Array[Double]]((0 to a.length).map(_ => new Array[Double](b.length + 1)): _*)
    matrix(0)(0) = 0
    for (i <- 1 to a.length) matrix(i)(0) = matrix(i - 1)(0) + weight(i)
    for (j <- 1 to b.length) matrix(0)(j) = matrix(0)(j - 1) + weight(j)

    for (j <- 1 to b.length; i <- 1 to a.length) {
      val substcost = (a(i - 1) == b(j - 1)) match {
        case true => 0
        case _ => weight(Math.max(i, j))
      }
      matrix(i)(j) = Array(matrix(i)(j - 1) + weight(j), matrix(i - 1)(j) + weight(i), matrix(i - 1)(j - 1) + substcost).min
    }

    matrix(a.length)(b.length)
  }

  protected def weight(idx: Int): Double = Math.exp(-0.05 * idx) + 0.05


}