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

class EqualInt(val target: Int, val entryWidth: Int) extends Predicate {

  val values = new Array[Int](entryWidth)
  val adds = new Array[Int](entryWidth)

  val targetMask = (1 << entryWidth) - 1

  {
    var origin = 0
    val lbEntryMask = targetMask >> 1
    var lbIntMask = 0
    for (i <- 0 to 32 / entryWidth) {
      origin = origin | (target << i * entryWidth)
      lbIntMask = lbIntMask | (lbEntryMask << i * entryWidth)
    }

    for (i <- 0 until entryWidth) {
      values(i) = (origin << i) | ((origin >> (32 - i)) & ((1 << i) - 1))
      adds(i) = (lbIntMask << i) | ((lbIntMask >> (32 - i)) & ((1 << i) - 1))
    }
  }


  def execute(input: ByteBuffer, offset: Int, size: Int): ByteBuffer = {
    val byteLength = Math.ceil((entryWidth * size.toDouble) / 32).toInt * 4
    val dest = ByteBuffer.allocateDirect(byteLength).order(ByteOrder.LITTLE_ENDIAN)

    val intLength = byteLength / 4

    input.position(offset)
    for (i <- 0 until intLength) {
      val bitoff = (entryWidth - (i * 32 % entryWidth)) % entryWidth
      val currentWord = input.getInt()
      val compare = currentWord ^ values(bitoff)

      var res = ((compare & adds(bitoff)) + adds(bitoff)) | compare

      // Deal with cross-boundary entry
      if (bitoff != 0) {
        val lastWord = input.getInt(i - 1)
        val lastOff = entryWidth - bitoff
        val combined = ((lastWord >> (32 - lastOff)) | (currentWord >> lastOff)) & targetMask
        if (combined != target) {
          res = res | (1 << (bitoff - 1))
        }
      }
      dest.putInt(res)
    }
    dest.flip()
    return dest
  }

}
