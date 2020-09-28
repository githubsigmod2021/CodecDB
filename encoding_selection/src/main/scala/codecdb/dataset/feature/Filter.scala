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

import scala.util.Random

/**
  * Stream Filters
  */
object Filter {

  def firstNFilter(n: Int): Iterator[String] => Iterator[String] = {
    (input: Iterator[String]) => {
      input.slice(0, n)
    }
  }

  def iidSamplingFilter(ratio: Double): Iterator[String] => Iterator[String] = {
    (input: Iterator[String]) => {
      input.filter(p => Random.nextDouble() <= ratio)
    }
  }

  def sizeFilter(size: Int): Iterator[String] => Iterator[String] = {
    (input: Iterator[String]) => {
      var counter = 0
      input.filter(record => {
        if (counter < size) {
          counter += record.size
          true
        } else
          false
      })
    }
  }

  def minSizeFilter(size: Int, ratio: Double): Iterator[String] => Iterator[String] = {
    (input: Iterator[String]) => {
      var counter = 0
      input.filter(record => {
        if (counter < size) {
          counter += record.length
          true
        } else {
          Random.nextDouble() <= ratio
        }
      })
    }
  }
}
