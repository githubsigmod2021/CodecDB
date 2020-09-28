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

package codecdb.query.offheap

import org.junit.Assert._
import org.junit.Test

import scala.util.Random

class EqualScalarTest {

  @Test
  def testPredicate: Unit = {

    val entryWidth = 6
    val pred = new EqualScalar(35, entryWidth)
    val data = Array(3, 1, 5, 2, 35, 0, 0, 1, 7, 35, 0, 0, 1, 2, 3, 35, 0, 0, 35, 1)
    val encoded = Encoder.encode(data, entryWidth)

    val result = pred.execute(encoded, 0, data.length)

    assertEquals(0x10.toByte, result.get(0))
    assertEquals(0x82.toByte, result.get(1))
    assertEquals(0x4.toByte, result.get(2))
  }

  @Test
  def testPredicateLong: Unit = {

    val entryWidth = 28
    val pred = new EqualScalar(35, entryWidth)
    val data = Array(3, 1, 5, 2, 35, 0, 0, 1, 7, 35, 0, 0, 1, 2, 3, 35, 0, 0, 35, 1)
    val encoded = Encoder.encode(data, entryWidth)

    val result = pred.execute(encoded, 0, data.length)

    assertEquals(0x10.toByte, result.get(0))
    assertEquals(0x82.toByte, result.get(1))
    assertEquals(0x4.toByte, result.get(2))
  }


  @Test
  def testPredicateManyData: Unit = {
    val dataLength = 3000;

    for (entryWidth <- 5 to 30) {
      val data = (0 until dataLength).map(i => Random.nextInt(30)).toArray
      val encoded = Encoder.encode(data, entryWidth)
      for (target <- 0 until 30) {
        val pred = new EqualScalar(target, entryWidth)
        val result = pred.execute(encoded, 0, data.length)

        for (i <- 0 until dataLength) {
          val byte = i / 8
          val bit = i % 8

          val compare = result.get(byte) & (1 << bit)

          compare match {
            case 0 => assertTrue("%d.%d".format(entryWidth, i), target != data(i))
            case _ => assertEquals("%d.%d".format(entryWidth, i), target, data(i))
          }
        }
      }
    }
  }
}
