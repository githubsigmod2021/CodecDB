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

import java.nio.{ByteBuffer, ByteOrder}

object Encoder {

  val boundary = 32

  def encode(input: Array[Int], entryWidth: Int): ByteBuffer = {
    val byteSize = Math.ceil((entryWidth * input.size).toDouble / boundary).toInt * boundary / 8

    val result = ByteBuffer.allocate(byteSize).order(ByteOrder.LITTLE_ENDIAN)

    var buffer = 0
    var offset = 0

    for (i <- input.indices) {
      buffer = buffer | (input(i) << offset)
      offset = offset + entryWidth
      if (offset >= boundary) {
        offset = offset - boundary
        result.putInt(buffer)
        buffer = input(i) >> (entryWidth - offset)
      }
    }
    if (offset != 0)
      result.putInt(buffer)
    result.flip()
    return result
  }
}
