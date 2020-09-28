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

import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.junit.Test

class EqualIntTest {

  @Test
  def testPredicate: Unit = {

    val entryWidth = 6
    val pred = new EqualInt(35, entryWidth)
    val data = Array(3, 1, 5, 2, 35, 0, 0, 1, 7, 35, 0, 0, 1, 2, 3, 35, 0, 0, 35, 1)
    val encoded = Encoder.encode(data, entryWidth)

    val result = pred.execute(encoded, 0, data.length)

    val mask = (1 << entryWidth) - 1

    for (i <- data.indices) {
      val bitcount = (i + 1) * entryWidth - 1
      val bitIndex = bitcount / 8
      val bitOffset = bitcount % 8

      val test = result.get(bitIndex) & (1 << bitOffset)
      if (data(i) == 35) {
        assertEquals(i.toString, 0, test)
      }
      else {
        assertThat(i.toString, 0, not(test))
      }
    }
  }
}
