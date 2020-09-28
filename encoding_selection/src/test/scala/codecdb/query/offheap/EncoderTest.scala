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

class EncoderTest {

  @Test
  def testEncode: Unit = {
    val result = Encoder.encode(Array(1, 3, 20, 10, 31, 1, 7, 56, 8, 22, 18, 36, 44, 9), 6)

    assertEquals(12, result.array().length)

    assertEquals(0x5f2940c1, result.getInt())
    assertEquals(0x2588e070, result.getInt())
    assertEquals(0x26c91, result.getInt())
  }

  @Test
  def testLongEncode: Unit = {

    val dataLength = 5000
    val data = (0 until dataLength).map(_ => Random.nextInt(100)).toArray

    for (entryWidth <- 10 to 30) {
      val result = Encoder.encode(data, entryWidth)
      val mask = (1 << entryWidth) - 1
      assertEquals(Math.ceil(dataLength * entryWidth.toDouble / 32).toInt * 4, result.array().length)

      data.indices.foreach(i => {
        val d = data(i)
        val byte = i * entryWidth / 8
        val bit = i * entryWidth % 8
        // No test for tail
        if (result.capacity() - byte >= 4) {
          val read = result.getInt(byte) >> bit
          assertEquals(d, read & mask)
        }
      })
    }
  }
}
