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

package codecdb.query.offheap

import java.nio.{ByteBuffer, ByteOrder}

class EqualLong(val target: Int, val entryWidth: Int) extends Predicate {

  val values = new Array[Long](entryWidth)
  val adds = new Array[Long](entryWidth)

  val targetMask = (1L << entryWidth) - 1

  {
    var origin = 0L
    val lbEntryMask = targetMask >> 1
    var lbIntMask = 0L
    for (i <- 0 to 64 / entryWidth) {
      origin = origin | (target << i * entryWidth)
      lbIntMask = lbIntMask | (lbEntryMask << i * entryWidth)
    }

    for (i <- 0 until entryWidth) {
      values(i) = (origin << i) | ((origin >> (64 - i)) & ((1 << i) - 1))
      adds(i) = (lbIntMask << i) | ((lbIntMask >> (64 - i)) & ((1 << i) - 1))
    }
  }


  def execute(input: ByteBuffer, offset: Int, size: Int): ByteBuffer = {
    val byteLength = Math.ceil((entryWidth * size.toDouble) / 64).toInt * 8
    val dest = ByteBuffer.allocateDirect(byteLength).order(ByteOrder.LITTLE_ENDIAN)

    val wordLength = byteLength / 8

    input.position(offset)
    for (i <- 0 until wordLength) {
      val bitoff = (entryWidth - (i * 64 % entryWidth)) % entryWidth
      val currentWord = input.getLong()
      val compare = currentWord ^ values(bitoff)

      var res = ((compare & adds(bitoff)) + adds(bitoff)) | compare

      // Deal with cross-boundary entry
      if (bitoff != 0) {
        val lastWord = input.getInt(i - 1)
        val lastOff = entryWidth - bitoff
        val combined = ((lastWord >> (64 - lastOff)) | (currentWord >> lastOff)) & targetMask
        if (combined != target) {
          res = res | (1L << (bitoff - 1))
        }
      }
      dest.putLong(res)
    }
    dest.flip()
    return dest
  }
}
